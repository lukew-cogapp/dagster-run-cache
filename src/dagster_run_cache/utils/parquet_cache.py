"""A cache backend kept as one Parquet file, answering a whole frame with one join.

Frame calls store the key columns and value columns as they are. Per-key calls use a
``key`` and a ``value`` column, so use one style per cache. Every write rewrites the file,
so per-key ``set`` suits small caches; bulk work goes through ``set_many`` or the frame calls.
"""

import contextlib
import os
import re
import tempfile
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

import polars as pl

from dagster_run_cache.utils.cache import Cache, Frame, Key, Lookup, null_counts, reject_null_keys

_NAME = re.compile(r"[\w.-]+")


@contextlib.contextmanager
def _atomic_write(path: Path) -> Iterator[str]:
    """Yield a temp path beside ``path`` that replaces it on success, so a reader never sees half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    os.close(fd)
    try:
        yield tmp
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def find_misses(name: str, frame: Frame, key: Key, stored: pl.LazyFrame | None) -> Lookup:
    """Split ``frame`` against the ``stored`` rows (``None`` when nothing is stored yet).

    The misses are collected, since they are what goes to the expensive call.
    """
    rows = frame.lazy()
    misses_lf = rows if stored is None else rows.join(stored.select(key), on=key, how="anti")
    distinct = rows.select(key).unique().select(pl.len())
    misses, total, nulls = pl.collect_all([misses_lf, distinct, null_counts(rows, key)])
    reject_null_keys(name, nulls)
    miss_count = misses.select(key).n_unique()
    return Lookup(name, misses, hit_count=total.item() - miss_count, miss_count=miss_count)


def merge_rows(name: str, frame: Frame, key: Key, stored: pl.LazyFrame | None) -> pl.LazyFrame | None:
    """Return ``stored`` with ``frame``'s rows added, replacing rows that share a key; ``None`` if nothing is new.

    The new rows are aligned to the stored schema (column order, then a strict cast), so
    compatible drift is absorbed and anything else raises.
    """
    new = frame.lazy().unique(key, keep="last", maintain_order=True).collect()
    if new.is_empty() and stored is not None:
        return None
    reject_null_keys(name, null_counts(new.lazy(), key).collect())
    if stored is None:
        return new.lazy()
    schema = stored.collect_schema()
    if set(schema.names()) != set(new.columns):
        raise ValueError(
            f"Cache {name!r} holds columns {sorted(schema.names())}, not {sorted(new.columns)}; "
            "clear it to start afresh"
        )
    try:
        rows = new.select(schema.names()).cast(schema).lazy()
    except pl.exceptions.InvalidOperationError as e:
        raise ValueError(f"Rows do not fit the dtypes stored in cache {name!r}: {dict(schema)}") from e
    return pl.concat([stored.join(rows.select(key), on=key, how="anti"), rows])


class ParquetCache(Cache):
    """One Parquet file at ``<base_dir>/<name>.parquet``, safe on a shared mount with one container per run.

    Two runs storing at once both succeed, but the later one drops the other's new rows,
    which costs a recompute rather than a corrupt file.
    """

    base_dir: str = "output/cache"

    def lookup(self, frame: Frame, key: Key) -> Lookup:
        """Find the rows of ``frame`` whose key is not cached, in one join; the misses are collected."""
        return find_misses(self.name, frame, key, self._stored())

    def store(self, frame: Frame, key: Key) -> None:
        """Add ``frame``'s rows, replacing rows that share a key; streams the old rows into a new file."""
        rows = merge_rows(self.name, frame, key, self._stored())
        if rows is None:
            return
        with _atomic_write(self.path) as tmp:
            rows.sink_parquet(tmp)

    def fetch(self, frame: Frame, key: Key) -> pl.LazyFrame:
        """Join the cached columns onto ``frame`` by key; rows not cached drop out."""
        stored = self._stored()
        if stored is None:
            return frame.lazy().head(0)
        return frame.lazy().join(stored, on=key, how="inner")

    def get_many(self, keys: Iterable[str]) -> dict[str, Any]:
        """Return the cached values for ``keys``, leaving out the misses."""
        fetched = self.fetch(pl.DataFrame({"key": list(keys)}, schema={"key": pl.String}), "key").collect()
        if fetched.is_empty():
            return {}
        return dict(zip(fetched["key"].to_list(), fetched["value"].to_list(), strict=True))

    def set_many(self, items: Mapping[str, Any]) -> None:
        """Store every key and value in ``items`` with one rewrite."""
        self.store(pl.DataFrame({"key": list(items), "value": list(items.values())}), "key")

    def delete(self, key: str) -> bool:
        """Remove ``key``; returns whether it was cached."""
        stored = self._stored()
        if stored is None or not self.has(key):
            return False
        with _atomic_write(self.path) as tmp:
            stored.filter(pl.col("key") != key).sink_parquet(tmp)
        return True

    def clear(self) -> int:
        """Remove the file; returns how many rows it held."""
        stored = self._stored()
        if stored is None:
            return 0
        count: int = stored.select(pl.len()).collect().item()
        self.path.unlink()
        return count

    @property
    def path(self) -> Path:
        """Where the cache file lives."""
        if not _NAME.fullmatch(self.name):
            raise ValueError(f"Cache name {self.name!r} must be letters, digits, '_', '.' or '-'")
        return Path(self.base_dir) / f"{self.name}.parquet"

    def _stored(self) -> pl.LazyFrame | None:
        return pl.scan_parquet(self.path) if self.path.exists() else None
