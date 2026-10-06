"""A cache interface for Dagster resources, with backends chosen per use.

Every backend answers per-key calls (``get``/``set``/``has``/``delete``, plus the bulk
``get_many``/``set_many``) and frame calls (``lookup``/``store``/``fetch``/``compute``). The
frame calls fall back to the bulk key calls here; a backend that stores tables, such as
``ParquetCache``, overrides them to answer a whole frame with one join.
"""

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import dagster as dg
import polars as pl
from pydantic import PrivateAttr

type Frame = pl.LazyFrame | pl.DataFrame
type Key = str | list[str]

_ROW_KEY = "__cache_key"
_MISSING = object()


def cache_metadata(name: str, hit_count: int, miss_count: int) -> dict[str, dg.MetadataValue]:
    """Hit and miss counts keyed by cache name, for ``context.add_output_metadata``."""
    return {
        f"cache/{name}/hits": dg.MetadataValue.int(hit_count),
        f"cache/{name}/misses": dg.MetadataValue.int(miss_count),
    }


@dataclass(frozen=True)
class Lookup:
    """The rows of a frame not yet cached, and how many distinct keys hit and missed."""

    name: str
    misses: pl.DataFrame
    hit_count: int
    miss_count: int

    @property
    def metadata(self) -> dict[str, dg.MetadataValue]:
        """Hit and miss counts keyed by cache name, for ``context.add_output_metadata``."""
        return cache_metadata(self.name, self.hit_count, self.miss_count)


def key_columns(key: Key) -> list[str]:
    """Return ``key`` as a list of column names."""
    return [key] if isinstance(key, str) else key


def null_counts(frame: pl.LazyFrame, key: Key) -> pl.LazyFrame:
    """Count nulls per key column, for ``reject_null_keys``."""
    return frame.select(pl.col(key_columns(key)).null_count())


def reject_null_keys(name: str, counts: pl.DataFrame) -> None:
    """Raise if any key column holds a null."""
    # Joins never match a null key, so a null-keyed row would miss on every run and drop out of fetch.
    if bad := [col for col, n in counts.row(0, named=True).items() if n]:
        raise ValueError(f"Key column(s) {bad} hold nulls; cache {name!r} cannot store a null key")


def _row_keys(frame: pl.LazyFrame, key: Key) -> pl.LazyFrame:
    # Key columns joined into one string, for backends that store string keys.
    parts = [pl.col(c).cast(pl.String) for c in key_columns(key)]
    return frame.with_columns(pl.concat_str(parts, separator="\x1f").alias(_ROW_KEY))


class Cache(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Base for cache backends; type a resource field as ``Cache`` to accept any of them."""

    name: str

    def get(self, key: str) -> Any | None:
        """Return the value for ``key``, or ``None`` if it is not cached."""
        return self.get_many([key]).get(key)

    def set(self, key: str, value: Any) -> None:
        """Store ``value`` under ``key``, replacing any existing value."""
        self.set_many({key: value})

    def has(self, key: str) -> bool:
        """Return whether ``key`` is cached."""
        return key in self.get_many([key])

    def delete(self, key: str) -> bool:
        """Remove ``key``; returns whether it was cached."""
        raise NotImplementedError

    def get_many(self, keys: Iterable[str]) -> dict[str, Any]:
        """Return the cached values for ``keys``, leaving out the misses."""
        raise NotImplementedError

    def set_many(self, items: Mapping[str, Any]) -> None:
        """Store every key and value in ``items``."""
        raise NotImplementedError

    def clear(self) -> int:
        """Remove everything; returns how many entries there were."""
        raise NotImplementedError

    def lookup(self, frame: Frame, key: Key) -> Lookup:
        """Find the rows of ``frame`` whose key is not cached; the misses are collected."""
        rows = frame.lazy()
        reject_null_keys(self.name, null_counts(rows, key).collect())
        keyed = _row_keys(rows, key).collect()
        found = self.get_many(keyed[_ROW_KEY].unique().to_list())
        misses = keyed.filter(~pl.col(_ROW_KEY).is_in(list(found)))
        miss_count = misses[_ROW_KEY].n_unique()
        return Lookup(self.name, misses.drop(_ROW_KEY), hit_count=len(found), miss_count=miss_count)

    def store(self, frame: Frame, key: Key) -> None:
        """Add ``frame``'s rows, replacing rows that share a key; non-key columns are the value."""
        rows = frame.lazy()
        reject_null_keys(self.name, null_counts(rows, key).collect())
        keyed = _row_keys(rows, key).collect().drop(key_columns(key))
        values = keyed.drop(_ROW_KEY).to_dicts()
        self.set_many(dict(zip(keyed[_ROW_KEY], values, strict=True)))

    def fetch(self, frame: Frame, key: Key) -> pl.LazyFrame:
        """Join the cached values onto ``frame`` by key; rows not cached drop out."""
        keyed = _row_keys(frame.lazy(), key)
        found = self.get_many(keyed.select(_ROW_KEY).unique().collect()[_ROW_KEY].to_list())
        if not found:
            return frame.lazy().head(0)
        values = pl.DataFrame({_ROW_KEY: list(found)}).hstack(pl.from_dicts(list(found.values())))
        return keyed.join(values.lazy(), on=_ROW_KEY, how="inner").drop(_ROW_KEY)

    def compute(
        self,
        frame: Frame,
        key: Key,
        fn: Callable[[pl.DataFrame], pl.DataFrame],
        batch_size: int | None = None,
    ) -> tuple[pl.LazyFrame, Lookup]:
        """Return ``frame`` joined to its cached values, calling ``fn`` only for keys not cached yet.

        ``fn`` gets the uncached keys and returns those key columns plus the columns to cache; it
        must return a row for every key it is given. With ``batch_size``, each batch is stored as
        it finishes, so a failure partway keeps the batches already done.
        """
        lookup = self.lookup(frame, key)
        keys = lookup.misses.select(key_columns(key)).unique(maintain_order=True)
        frame_columns = frozenset(frame.lazy().collect_schema().names())
        log = dg.get_dagster_logger()
        for n, batch in enumerate(keys.iter_slices(batch_size or max(keys.height, 1)), start=1):
            log.info(f"Cache {self.name}: computing batch {n}, {batch.height} keys")
            result = fn(batch)
            self._check_result(batch, result, key, frame_columns)
            self.store(result, key)
        return self.fetch(frame, key), lookup

    def _check_result(self, batch: pl.DataFrame, result: pl.DataFrame, key: Key, frame_columns: frozenset[str]) -> None:
        columns = key_columns(key)
        if absent := [c for c in columns if c not in result.columns]:
            raise ValueError(f"fn for cache {self.name!r} must return the key column(s) {absent}")
        if clashing := sorted((set(result.columns) - set(columns)) & frame_columns):
            raise ValueError(f"fn for cache {self.name!r} returned {clashing}, which the input frame already has")
        dropped = batch.join(result.select(columns), on=columns, how="anti")
        if not dropped.is_empty():
            raise ValueError(
                f"fn for cache {self.name!r} returned no row for {dropped.height} of {batch.height} keys, "
                f"e.g. {dropped.row(0, named=True)}"
            )


class MemoryCache(Cache):
    """A dict that lasts as long as the resource instance: one run, or one test."""

    _items: dict[str, Any] = PrivateAttr(default_factory=dict)

    def delete(self, key: str) -> bool:
        """Remove ``key``; returns whether it was cached."""
        return self._items.pop(key, _MISSING) is not _MISSING

    def get_many(self, keys: Iterable[str]) -> dict[str, Any]:
        """Return the cached values for ``keys``, leaving out the misses."""
        return {k: self._items[k] for k in keys if k in self._items}

    def set_many(self, items: Mapping[str, Any]) -> None:
        """Store every key and value in ``items``."""
        self._items.update(items)

    def clear(self) -> int:
        """Remove everything; returns how many entries there were."""
        count = len(self._items)
        self._items.clear()
        return count
