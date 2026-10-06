"""Run the pipeline four times against one storage dir and print what the cache saved each time.

Each run uses a fresh ephemeral Dagster instance, so the only thing carried
between runs is the Parquet on disk, as with a container-per-run deployment.

    uv run python scripts/demo.py
"""

import shutil
import sys
import time
from pathlib import Path

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager

from cached_compute_demo.defs import doc_embeddings, documents

STORAGE = Path(__file__).resolve().parent.parent / "output"

RUNS = [
    ("first run, empty cache", 1, "fake-embed-v1"),
    ("nothing changed", 1, "fake-embed-v1"),
    ("source edited: 10 retitled, 5 deleted, 5 added", 2, "fake-embed-v1"),
    ("embedding model bumped", 2, "fake-embed-v2"),
]


def main() -> None:
    """Wipe the storage dir, then materialise once per entry in ``RUNS``."""
    shutil.rmtree(STORAGE, ignore_errors=True)
    io_manager = PolarsParquetIOManager(base_dir=str(STORAGE))

    for n, (label, edition, version) in enumerate(RUNS, start=1):
        print(f"Run {n}: {label}…", flush=True)
        started = time.perf_counter()
        result = dg.materialize(
            [documents, doc_embeddings],
            resources={"io_manager": io_manager},
            run_config={
                "ops": {
                    "documents": {"config": {"edition": edition}},
                    "doc_embeddings": {"config": {"version": version}},
                }
            },
        )
        elapsed = time.perf_counter() - started
        if not result.success:
            sys.exit(f"Run {n} failed")
        meta = result.asset_materializations_for_node("doc_embeddings")[0].metadata
        rows = pl.scan_parquet(STORAGE / "doc_embeddings.parquet").select(pl.len()).collect().item()
        print(
            f"  hits {meta['cache_hits'].value:>5}  misses {meta['cache_misses'].value:>5}  "
            f"rows {rows:>5}  {elapsed:5.2f}s",
            flush=True,
        )


if __name__ == "__main__":
    main()
