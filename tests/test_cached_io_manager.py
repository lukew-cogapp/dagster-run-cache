from pathlib import Path

import dagster as dg
import polars as pl

from dagster_run_cache.utils.cached_io_manager import CachedParquetIOManager, uncached

KEYS: list[str] = []


@dg.asset(io_manager_key="cached_io_manager", metadata={"cache_key": "k"})
def lengths(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Return the length of each uncached key in ``KEYS``."""
    lookup = uncached(context, pl.DataFrame({"k": KEYS}))
    context.add_output_metadata(lookup.metadata)
    return lookup.misses.with_columns(v=pl.col("k").str.len_chars())


@dg.asset(io_manager_key="cached_io_manager")
def no_key() -> pl.DataFrame:
    """Return rows without declaring a cache key."""
    return pl.DataFrame({"k": ["a"]})


def _materialise(tmp_path: Path, keys: list[str], asset: dg.AssetsDefinition = lengths) -> dg.ExecuteInProcessResult:
    KEYS[:] = keys
    return dg.materialize(
        [asset], resources={"cached_io_manager": CachedParquetIOManager(base_dir=str(tmp_path))}, raise_on_error=False
    )


def _stored(tmp_path: Path) -> pl.DataFrame:
    return pl.read_parquet(tmp_path / "lengths.parquet").sort("k")


def test_new_rows_are_merged_into_the_stored_output(tmp_path: Path) -> None:
    """A second run returns only its new key, and the stored file holds both runs' rows."""
    _materialise(tmp_path, ["a", "bb"])
    result = _materialise(tmp_path, ["a", "ccc"])

    meta = result.asset_materializations_for_node("lengths")[0].metadata
    assert (meta["cache/lengths/hits"].value, meta["cache/lengths/misses"].value) == (1, 1)
    assert meta["cache/rows_stored"].value == 3
    assert _stored(tmp_path).to_dicts() == [{"k": "a", "v": 1}, {"k": "bb", "v": 2}, {"k": "ccc", "v": 3}]


def test_run_with_nothing_new_leaves_the_file_alone(tmp_path: Path) -> None:
    """When every key is stored already, the asset returns no rows and the file is untouched."""
    _materialise(tmp_path, ["a"])
    before = (tmp_path / "lengths.parquet").stat().st_mtime_ns
    _materialise(tmp_path, ["a"])
    assert (tmp_path / "lengths.parquet").stat().st_mtime_ns == before
    assert [p.name for p in tmp_path.iterdir()] == ["lengths.parquet"]


def test_asset_without_cache_key_fails(tmp_path: Path) -> None:
    """Storing through the cached IO manager without ``cache_key`` metadata raises."""
    result = _materialise(tmp_path, [], asset=no_key)
    assert not result.success
    assert "cache_key" in str(result.get_step_failure_events()[0].event_specific_data.error)  # type: ignore[union-attr]
