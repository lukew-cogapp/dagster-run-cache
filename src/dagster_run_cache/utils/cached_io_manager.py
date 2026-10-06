"""A Parquet IO manager whose assets add rows to their stored output instead of replacing it.

The asset is the cache: it returns only the rows it computed this run, and the IO manager
merges them into what it stored before. Paths, S3 options and loading come from dagster-polars.
"""

import uuid
from collections.abc import Mapping
from typing import Any

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager
from dagster_polars.io_managers.parquet import _remap_fsspec_storage_options
from upath import UPath

from dagster_run_cache.utils.cache import Frame, Key, Lookup, find_misses, merge_rows

CACHE_KEY = "cache_key"


def _cache_key(metadata: Mapping[str, Any] | None, asset: str) -> Key:
    key = (metadata or {}).get(CACHE_KEY)
    if not key:
        raise ValueError(f"Asset {asset!r} uses the cached IO manager but declares no {CACHE_KEY!r} metadata")
    return list(key) if not isinstance(key, str) else key


def uncached(context: dg.AssetExecutionContext, frame: Frame) -> Lookup:
    """Return the rows of ``frame`` this asset has not stored yet, by the asset's ``cache_key`` metadata."""
    name = context.asset_key.to_user_string()
    key = _cache_key(context.assets_def.specs_by_key[context.asset_key].metadata, name)
    try:
        stored = context.load_asset_value(context.asset_key, python_type=pl.LazyFrame)
    except FileNotFoundError:
        stored = None
    return find_misses(name, frame, key, stored)


class CachedParquetIOManager(PolarsParquetIOManager):
    """Stores an asset's returned rows merged into its previous output, replacing rows that share a key.

    The asset declares its key as ``metadata={"cache_key": [...]}`` and calls ``uncached`` to find
    the rows it still has to compute. A run with nothing new writes nothing.
    """

    def dump_to_path(self, context: dg.OutputContext, obj: Any, path: UPath) -> None:
        """Merge ``obj`` into the file at ``path`` through a temp file beside it."""
        name = context.asset_key.to_user_string()
        stored = self._scan(path) if path.exists() else None
        rows = merge_rows(name, obj, _cache_key(context.definition_metadata, name), stored)
        if rows is None:
            return
        # Written beside the target and renamed over it: the merge reads the file it replaces.
        tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            self.sink_df_to_path(context, rows, tmp)
            tmp.rename(path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        context.add_output_metadata({"cache/rows_stored": dg.MetadataValue.int(self._row_count(path))})

    def _scan(self, path: UPath) -> pl.LazyFrame:
        return pl.scan_parquet(str(path), storage_options=_remap_fsspec_storage_options(self.storage_options))

    def _row_count(self, path: UPath) -> int:
        count: int = self._scan(path).select(pl.len()).collect().item()
        return count
