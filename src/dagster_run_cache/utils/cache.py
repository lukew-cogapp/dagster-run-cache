"""A cache of computed rows that persists between Dagster runs.

Each table is one Parquet file under ``base_dir``, keyed by one or more columns.
"""

import contextlib
import os
import re
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import dagster as dg
import polars as pl

type Frame = pl.LazyFrame | pl.DataFrame
type Key = str | list[str]

_TABLE_NAME = re.compile(r"[\w.-]+")


@dataclass(frozen=True)
class Lookup:
    """The rows of a frame not yet cached, and how many distinct keys hit and missed."""

    table: str
    misses: pl.DataFrame
    hit_count: int
    miss_count: int

    @property
    def metadata(self) -> dict[str, dg.MetadataValue]:
        """Hit and miss counts keyed by table, for ``context.add_output_metadata``."""
        return {
            f"cache/{self.table}/hits": dg.MetadataValue.int(self.hit_count),
            f"cache/{self.table}/misses": dg.MetadataValue.int(self.miss_count),
        }


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


def _key_columns(key: Key) -> list[str]:
    return [key] if isinstance(key, str) else key


def _null_counts(frame: pl.LazyFrame, key: Key) -> pl.LazyFrame:
    return frame.select(pl.col(_key_columns(key)).null_count())


def _reject_null_keys(table: str, null_counts: pl.DataFrame) -> None:
    # Joins never match a null key, so a null-keyed row would miss on every run and drop out of fetch.
    if bad := [col for col, n in null_counts.row(0, named=True).items() if n]:
        raise ValueError(f"Key column(s) {bad} hold nulls; table {table!r} cannot cache a null key")


def find_misses(table: str, frame: Frame, key: Key, stored: pl.LazyFrame | None) -> Lookup:
    """Split ``frame`` against the ``stored`` rows of ``table`` (``None`` when nothing is stored yet).

    The misses are collected, since they are what goes to the expensive call.
    """
    rows = frame.lazy()
    misses_lf = rows if stored is None else rows.join(stored.select(key), on=key, how="anti")
    distinct = rows.select(key).unique().select(pl.len())
    misses, total, nulls = pl.collect_all([misses_lf, distinct, _null_counts(rows, key)])
    _reject_null_keys(table, nulls)
    miss_count = misses.select(key).n_unique()
    return Lookup(table, misses, hit_count=total.item() - miss_count, miss_count=miss_count)


def merge_rows(table: str, frame: Frame, key: Key, stored: pl.LazyFrame | None) -> pl.LazyFrame | None:
    """Return ``stored`` with ``frame``'s rows added, replacing rows that share a key; ``None`` if nothing is new.

    The new rows are aligned to the stored schema (column order, then a strict cast), so
    compatible drift is absorbed and anything else raises.
    """
    new = frame.lazy().unique(key, keep="last", maintain_order=True).collect()
    if new.is_empty() and stored is not None:
        return None
    _reject_null_keys(table, _null_counts(new.lazy(), key).collect())
    if stored is None:
        return new.lazy()
    schema = stored.collect_schema()
    if set(schema.names()) != set(new.columns):
        raise ValueError(
            f"Table {table!r} holds columns {sorted(schema.names())}, not {sorted(new.columns)}; "
            "clear it to start afresh"
        )
    try:
        rows = new.select(schema.names()).cast(schema).lazy()
    except pl.exceptions.InvalidOperationError as e:
        raise ValueError(f"Rows do not fit the dtypes stored in table {table!r}: {dict(schema)}") from e
    return pl.concat([stored.join(rows.select(key), on=key, how="anti"), rows])


class RunCache(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Computed rows shared by every run pointing at the same ``base_dir``, one file per table."""

    base_dir: str = "output/cache"

    def compute(
        self,
        table: str,
        frame: Frame,
        key: Key,
        fn: Callable[[pl.DataFrame], pl.DataFrame],
        batch_size: int | None = None,
    ) -> tuple[pl.LazyFrame, Lookup]:
        """Return ``frame`` joined to its cached columns, calling ``fn`` only for keys not cached yet.

        ``fn`` gets the uncached keys and returns those key columns plus the columns to cache; it
        must return a row for every key it is given. With ``batch_size``, each batch is stored as
        it finishes, so a failure partway keeps the batches already done.
        """
        lookup = self.lookup(table, frame, key)
        keys = lookup.misses.select(_key_columns(key)).unique(maintain_order=True)
        frame_columns = set(frame.lazy().collect_schema().names())
        log = dg.get_dagster_logger()
        for n, batch in enumerate(keys.iter_slices(batch_size or max(keys.height, 1)), start=1):
            log.info(f"Cache {table}: computing batch {n}, {batch.height} keys")
            result = fn(batch)
            self._check_result(table, batch, result, key, frame_columns)
            self.store(table, result, key)
        return self.fetch(table, frame, key), lookup

    def lookup(self, table: str, frame: Frame, key: Key) -> Lookup:
        """Find the rows of ``frame`` whose key is not cached in ``table``; the misses are collected."""
        return find_misses(table, frame, key, self._stored(table))

    def store(self, table: str, frame: Frame, key: Key) -> None:
        """Add ``frame``'s rows to ``table``, replacing rows that share a key.

        Rewrites the file, streaming the old rows with the new so memory stays bounded.
        Two runs storing at once both succeed, but the later one drops the other's
        new rows, which costs a recompute rather than a corrupt cache.
        """
        rows = merge_rows(table, frame, key, self._stored(table))
        if rows is None:
            return
        with _atomic_write(self._path(table)) as tmp:
            rows.sink_parquet(tmp)

    def fetch(self, table: str, frame: Frame, key: Key) -> pl.LazyFrame:
        """Join ``table``'s cached columns onto ``frame`` by key; rows not cached drop out."""
        path = self._path(table)
        if not path.exists():
            return frame.lazy().head(0)
        return frame.lazy().join(pl.scan_parquet(path), on=key, how="inner")

    def clear(self, table: str) -> int:
        """Remove ``table``; returns how many rows it held."""
        path = self._path(table)
        if not path.exists():
            return 0
        count: int = pl.scan_parquet(path).select(pl.len()).collect().item()
        path.unlink()
        return count

    def _check_result(
        self, table: str, batch: pl.DataFrame, result: pl.DataFrame, key: Key, frame_columns: set[str]
    ) -> None:
        key_columns = _key_columns(key)
        if absent := [c for c in key_columns if c not in result.columns]:
            raise ValueError(f"fn for table {table!r} must return the key column(s) {absent}")
        if clashing := sorted((set(result.columns) - set(key_columns)) & frame_columns):
            raise ValueError(f"fn for table {table!r} returned {clashing}, which the input frame already has")
        dropped = batch.join(result.select(key_columns), on=key_columns, how="anti")
        if not dropped.is_empty():
            raise ValueError(
                f"fn for table {table!r} returned no row for {dropped.height} of {batch.height} keys, "
                f"e.g. {dropped.row(0, named=True)}"
            )

    def _stored(self, table: str) -> pl.LazyFrame | None:
        path = self._path(table)
        return pl.scan_parquet(path) if path.exists() else None

    def _path(self, table: str) -> Path:
        if not _TABLE_NAME.fullmatch(table):
            raise ValueError(f"Cache table name {table!r} must be letters, digits, '_', '.' or '-'")
        return Path(self.base_dir) / f"{table}.parquet"
