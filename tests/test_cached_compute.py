from pathlib import Path

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager

from cached_compute_demo.cached_compute import HASH_COLUMN, input_hash
from cached_compute_demo.defs import ALL_ASSETS
from cached_compute_demo.fakes import fake_documents

CACHED = ["doc_embeddings", "place_geocodes", "image_analysis"]


def _run(storage: Path, edition: int = 1, model: str = "v1") -> dict[str, tuple[int, int]]:
    result = dg.materialize(
        ALL_ASSETS,
        resources={"io_manager": PolarsParquetIOManager(base_dir=str(storage))},
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


def _output(storage: Path, asset: str, key: str) -> pl.DataFrame:
    return pl.read_parquet(storage / f"{asset}.parquet").sort(key)


def test_first_run_computes_every_row(tmp_path: Path) -> None:
    """With no prior output, every row of every cached asset is a miss."""
    assert _run(tmp_path) == {
        "doc_embeddings": (0, 100),
        "place_geocodes": (0, 12),
        "image_analysis": (0, 100),
    }


def test_unchanged_rerun_computes_nothing(tmp_path: Path) -> None:
    """A rerun over identical input is all hits and leaves the output unchanged."""
    _run(tmp_path)
    before = _output(tmp_path, "doc_embeddings", "doc_id")
    assert _run(tmp_path) == {
        "doc_embeddings": (100, 0),
        "place_geocodes": (12, 0),
        "image_analysis": (100, 0),
    }
    assert _output(tmp_path, "doc_embeddings", "doc_id").equals(before)


def test_edits_recompute_only_affected_rows(tmp_path: Path) -> None:
    """Each asset recomputes only rows whose own inputs changed in the edition."""
    _run(tmp_path)
    assert _run(tmp_path, edition=2) == {
        "doc_embeddings": (85, 15),  # 10 retitled + 5 added
        "place_geocodes": (12, 1),  # the new place
        "image_analysis": (92, 8),  # 3 re-photographed + 5 added
    }


def test_deleted_rows_leave_the_output(tmp_path: Path) -> None:
    """A key absent from the input is dropped, and untouched rows carry over unchanged."""
    _run(tmp_path)
    before = _output(tmp_path, "doc_embeddings", "doc_id")
    _run(tmp_path, edition=2)
    after = _output(tmp_path, "doc_embeddings", "doc_id")

    assert after["doc_id"].equals(fake_documents(edition=2, size=100)["doc_id"].sort())
    untouched = before.join(after, on=["doc_id", HASH_COLUMN], how="semi")
    assert untouched.height == 85
    assert untouched.equals(after.join(untouched.select("doc_id"), on="doc_id", how="semi"))


def test_version_bump_invalidates_only_that_asset(tmp_path: Path) -> None:
    """A new embedding model recomputes every vector and leaves the other caches alone."""
    _run(tmp_path)
    assert _run(tmp_path, model="v2") == {
        "doc_embeddings": (0, 100),
        "place_geocodes": (12, 0),
        "image_analysis": (100, 0),
    }


def test_input_hash_depends_on_inputs_and_version() -> None:
    """The hash is deterministic and moves with either the input value or the version."""
    df = pl.DataFrame({"title": ["a", "a", "b"]})
    v1 = df.select(input_hash(["title"], "v1"))[HASH_COLUMN]
    v2 = df.select(input_hash(["title"], "v2"))[HASH_COLUMN]
    assert v1[0] == v1[1]
    assert v1[0] != v1[2]
    assert v1[0] != v2[0]
