"""A cache of computed rows that persists between Dagster runs.

Each prefix is one Parquet file under ``base_dir``, keyed by one or more
columns. A lookup is a single scan and join, and a store rewrites the file
through a temp file, so the cache works on a network mount shared by one
container per run.
"""

import contextlib
import os
import re
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path

import dagster as dg
import polars as pl
from pydantic import PrivateAttr

type Frame = pl.LazyFrame | pl.DataFrame
type Key = str | list[str]

_PREFIX = re.compile(r"[\w.-]+")


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


class RunCache(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Computed rows shared by every run pointing at the same ``base_dir``, one file per prefix."""

    base_dir: str = "output/cache"

    _hits: int = PrivateAttr(default=0)
    _misses: int = PrivateAttr(default=0)

    def compute(self, prefix: str, frame: Frame, key: Key, fn: Callable[[pl.DataFrame], pl.DataFrame]) -> pl.DataFrame:
        """Return ``frame`` joined to its cached columns, calling ``fn`` only for keys not cached yet.

        ``fn`` gets the uncached rows, one per key, and returns the key columns plus the values to cache.
        """
        misses = self.missing(prefix, frame, key)
        if not misses.is_empty():
            self.store(prefix, fn(misses.unique(key, maintain_order=True)), key)
        return self.fetch(prefix, frame, key).collect()

    def missing(self, prefix: str, frame: Frame, key: Key) -> pl.DataFrame:
        """Return the rows of ``frame`` whose key is not cached under ``prefix``.

        Collected, since the misses are what goes to the expensive call. Counts towards ``take_stats``.
        """
        rows = frame.lazy().collect()
        misses = rows
        path = self._path(prefix)
        if path.exists():
            misses = rows.lazy().join(pl.scan_parquet(path).select(key), on=key, how="anti").collect()
        self._misses += misses.height
        self._hits += rows.height - misses.height
        return misses

    def store(self, prefix: str, frame: Frame, key: Key) -> None:
        """Add ``frame``'s rows under ``prefix``, replacing rows that share a key.

        Rewrites the file: the old rows stream with the new into a temp file that
        then replaces it, so memory stays bounded and readers never see half a file.
        Two runs storing at once both succeed, but the later one drops the other's
        new rows, which costs a recompute rather than a corrupt cache.
        """
        new = frame.lazy().unique(key, keep="last", maintain_order=True).collect()
        if new.is_empty():
            return
        path = self._path(prefix)
        rows = new.lazy()
        if path.exists():
            old = pl.scan_parquet(path)
            if old.collect_schema() != new.schema:
                raise ValueError(
                    f"Columns stored under {prefix!r} have changed; call clear({prefix!r}) to start afresh"
                )
            rows = pl.concat([old.join(rows.select(key), on=key, how="anti"), rows])
        with _atomic_write(path) as tmp:
            rows.sink_parquet(tmp)

    def fetch(self, prefix: str, frame: Frame, key: Key) -> pl.LazyFrame:
        """Join the columns cached under ``prefix`` onto ``frame`` by key; rows not cached drop out."""
        path = self._path(prefix)
        if not path.exists():
            return frame.lazy().head(0)
        return frame.lazy().join(pl.scan_parquet(path), on=key, how="inner")

    def clear(self, prefix: str) -> int:
        """Remove everything cached under ``prefix``; returns how many rows there were."""
        path = self._path(prefix)
        if not path.exists():
            return 0
        count: int = pl.scan_parquet(path).select(pl.len()).collect().item()
        path.unlink()
        return count

    def take_stats(self) -> dict[str, int]:
        """Return hit and miss counts since the last call, then reset them."""
        stats = {"cache_hits": self._hits, "cache_misses": self._misses}
        self._hits = self._misses = 0
        return stats

    def _path(self, prefix: str) -> Path:
        if not _PREFIX.fullmatch(prefix):
            raise ValueError(f"Cache prefix {prefix!r} must be letters, digits, '_', '.' or '-'")
        return Path(self.base_dir) / f"{prefix}.parquet"
