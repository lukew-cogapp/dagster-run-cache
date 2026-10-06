from pathlib import Path

import polars as pl

from dagster_run_cache.defs import run_demo
from dagster_run_cache.fakes import fake_documents


def _run(storage: Path, edition: int = 1, model: str = "v1") -> dict[str, tuple[int, int]]:
    return run_demo(storage, edition=edition, model=model, size=100)


def _embeddings(storage: Path) -> pl.DataFrame:
    return pl.read_parquet(storage / "doc_embeddings.parquet").sort("doc_id")


def test_first_run_computes_everything(tmp_path: Path) -> None:
    """With an empty cache, every lookup is a miss."""
    assert _run(tmp_path) == {
        "place_geocodes": (0, 12),
        "doc_embeddings": (0, 100),
        "doc_embeddings_by_id": (0, 100),
        "image_analysis": (0, 100),
    }


def test_unchanged_rerun_computes_nothing(tmp_path: Path) -> None:
    """A rerun over identical input is all hits and produces identical output."""
    _run(tmp_path)
    before = _embeddings(tmp_path)
    assert _run(tmp_path) == {
        "place_geocodes": (12, 0),
        "doc_embeddings": (100, 0),
        "doc_embeddings_by_id": (100, 0),
        "image_analysis": (100, 0),
    }
    assert _embeddings(tmp_path).equals(before)


def test_edits_recompute_only_affected_keys(tmp_path: Path) -> None:
    """Each asset misses only on keys whose inputs changed in the edition."""
    _run(tmp_path)
    assert _run(tmp_path, edition=2) == {
        "place_geocodes": (12, 1),  # the new place
        "doc_embeddings": (85, 15),  # 10 retitled + 5 added
        "doc_embeddings_by_id": (85, 15),  # the same 15 carry a newer modified date
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
    """A new model name misses every key in both embedding tables and leaves the other assets cached."""
    _run(tmp_path)
    assert _run(tmp_path, model="v2") == {
        "place_geocodes": (12, 0),
        "doc_embeddings": (0, 100),
        "doc_embeddings_by_id": (0, 100),
        "image_analysis": (100, 0),
    }


def test_id_key_gives_the_same_vectors_without_storing_text(tmp_path: Path) -> None:
    """Keying on id and modified date reproduces the text-keyed vectors, and its table holds no text."""
    _run(tmp_path)
    _run(tmp_path, edition=2)
    by_id = pl.read_parquet(tmp_path / "doc_embeddings_by_id.parquet").sort("doc_id")
    assert by_id.equals(_embeddings(tmp_path))
    assert pl.read_parquet_schema(tmp_path / "cache" / "embed_by_id.parquet").keys() == {
        "model",
        "doc_id",
        "modified",
        "vector",
    }
