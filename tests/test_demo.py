from pathlib import Path

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager

from dagster_run_cache import RunCache
from dagster_run_cache.defs import ALL_ASSETS
from dagster_run_cache.fakes import fake_documents

CACHED = ["place_geocodes", "doc_embeddings", "image_analysis"]


def _run(storage: Path, edition: int = 1, model: str = "v1") -> dict[str, tuple[int, int]]:
    result = dg.materialize(
        ALL_ASSETS,
        resources={
            "io_manager": PolarsParquetIOManager(base_dir=str(storage)),
            "cache": RunCache(base_dir=str(storage / "cache")),
        },
        run_config={
            "ops": {
                "documents": {"config": {"edition": edition, "size": 100}},
                "doc_embeddings": {"config": {"model": model}},
            }
        },
    )
    assert result.success
    counts = {}
    for asset in CACHED:
        meta = result.asset_materializations_for_node(asset)[0].metadata
        counts[asset] = (meta["cache_hits"].value, meta["cache_misses"].value)
    return counts  # type: ignore[return-value]


def _embeddings(storage: Path) -> pl.DataFrame:
    return pl.read_parquet(storage / "doc_embeddings.parquet").sort("doc_id")


def test_first_run_computes_everything(tmp_path: Path) -> None:
    """With an empty cache, every lookup is a miss."""
    assert _run(tmp_path) == {
        "place_geocodes": (0, 12),
        "doc_embeddings": (0, 100),
        "image_analysis": (0, 100),
    }


def test_unchanged_rerun_computes_nothing(tmp_path: Path) -> None:
    """A rerun over identical input is all hits and produces identical output."""
    _run(tmp_path)
    before = _embeddings(tmp_path)
    assert _run(tmp_path) == {
        "place_geocodes": (12, 0),
        "doc_embeddings": (100, 0),
        "image_analysis": (100, 0),
    }
    assert _embeddings(tmp_path).equals(before)


def test_edits_recompute_only_affected_keys(tmp_path: Path) -> None:
    """Each asset misses only on keys whose inputs changed in the edition."""
    _run(tmp_path)
    assert _run(tmp_path, edition=2) == {
        "place_geocodes": (12, 1),  # the new place
        "doc_embeddings": (85, 15),  # 10 retitled + 5 added
        "image_analysis": (92, 8),  # 3 re-photographed + 5 added
    }


def test_output_follows_the_current_source(tmp_path: Path) -> None:
    """Deleted documents leave the output, and untouched vectors come back unchanged."""
    _run(tmp_path)
    before = _embeddings(tmp_path)
    _run(tmp_path, edition=2)
    after = _embeddings(tmp_path)

    assert after["doc_id"].equals(fake_documents(edition=2, size=100)["doc_id"].sort())
    untouched = list(range(10, 95))
    assert after.filter(pl.col("doc_id").is_in(untouched)).equals(before.filter(pl.col("doc_id").is_in(untouched)))


def test_model_bump_invalidates_only_embeddings(tmp_path: Path) -> None:
    """A new model name misses every embedding key and leaves the other assets cached."""
    _run(tmp_path)
    assert _run(tmp_path, model="v2") == {
        "place_geocodes": (12, 0),
        "doc_embeddings": (0, 100),
        "image_analysis": (100, 0),
    }
