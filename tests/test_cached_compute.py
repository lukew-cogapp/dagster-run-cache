from pathlib import Path

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager

from cached_compute_demo.cached_compute import HASH_COLUMN, input_hash
from cached_compute_demo.defs import doc_embeddings, documents, fake_documents


def _run(storage: Path, edition: int = 1, version: str = "v1") -> dict[str, int]:
    result = dg.materialize(
        [documents, doc_embeddings],
        resources={"io_manager": PolarsParquetIOManager(base_dir=str(storage))},
        run_config={
            "ops": {
                "documents": {"config": {"edition": edition, "size": 100}},
                "doc_embeddings": {"config": {"version": version}},
            }
        },
    )
    assert result.success
    meta = result.asset_materializations_for_node("doc_embeddings")[0].metadata
    return {"hits": meta["cache_hits"].value, "misses": meta["cache_misses"].value}  # type: ignore[dict-item]


def _output(storage: Path) -> pl.DataFrame:
    return pl.read_parquet(storage / "doc_embeddings.parquet").sort("doc_id")


def test_first_run_computes_every_row(tmp_path: Path) -> None:
    """With no prior output, every row is a miss."""
    assert _run(tmp_path) == {"hits": 0, "misses": 100}


def test_unchanged_rerun_computes_nothing(tmp_path: Path) -> None:
    """A rerun over identical input is all hits and leaves the output unchanged."""
    _run(tmp_path)
    before = _output(tmp_path)
    assert _run(tmp_path) == {"hits": 100, "misses": 0}
    assert _output(tmp_path).equals(before)


def test_edits_recompute_only_changed_rows(tmp_path: Path) -> None:
    """Retitled and added rows are misses; deleted rows leave the output; untouched vectors carry over."""
    _run(tmp_path)
    before = _output(tmp_path)
    assert _run(tmp_path, edition=2) == {"hits": 85, "misses": 15}

    after = _output(tmp_path)
    expected_ids = fake_documents(edition=2, size=100)["doc_id"].sort()
    assert after["doc_id"].equals(expected_ids)

    untouched = before.join(after, on=["doc_id", HASH_COLUMN], how="semi")
    assert untouched.height == 85
    assert untouched.equals(after.join(untouched.select("doc_id"), on="doc_id", how="semi"))


def test_version_bump_invalidates_every_row(tmp_path: Path) -> None:
    """Changing the version salt turns every row into a miss."""
    _run(tmp_path)
    assert _run(tmp_path, version="v2") == {"hits": 0, "misses": 100}


def test_input_hash_depends_on_inputs_and_version() -> None:
    """The hash is deterministic and moves with either the input value or the version."""
    df = pl.DataFrame({"title": ["a", "a", "b"]})
    v1 = df.select(input_hash(["title"], "v1"))[HASH_COLUMN]
    v2 = df.select(input_hash(["title"], "v2"))[HASH_COLUMN]
    assert v1[0] == v1[1]
    assert v1[0] != v1[2]
    assert v1[0] != v2[0]
