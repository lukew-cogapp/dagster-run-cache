"""Run the pipeline four times against one storage dir and print what each cached asset saved.

Each run uses a fresh ephemeral Dagster instance, so the only thing carried
between runs is the Parquet on disk, as with a container-per-run deployment.

    uv run python scripts/demo.py
"""

import shutil
import sys
import time
from pathlib import Path

import dagster as dg
from dagster_polars import PolarsParquetIOManager

from cached_compute_demo.defs import ALL_ASSETS

STORAGE = Path(__file__).resolve().parent.parent / "output"
CACHED = ["doc_embeddings", "place_geocodes", "image_analysis"]

RUNS = [
    ("first run, empty cache", 1, "fake-embed-v1"),
    ("nothing changed", 1, "fake-embed-v1"),
    ("source edited: 10 retitled, 3 re-photographed, 5 deleted, 5 added in a new place", 2, "fake-embed-v1"),
    ("embedding model bumped", 2, "fake-embed-v2"),
]


def main() -> None:
    """Wipe the storage dir, then materialise once per entry in ``RUNS``."""
    shutil.rmtree(STORAGE, ignore_errors=True)
    io_manager = PolarsParquetIOManager(base_dir=str(STORAGE))

    for n, (label, edition, model) in enumerate(RUNS, start=1):
        print(f"\nRun {n}: {label}…", flush=True)
        started = time.perf_counter()
        result = dg.materialize(
            ALL_ASSETS,
            resources={"io_manager": io_manager},
            run_config={
                "ops": {
                    "documents": {"config": {"edition": edition}},
                    "doc_embeddings": {"config": {"model": model}},
                }
            },
        )
        if not result.success:
            sys.exit(f"Run {n} failed")
        for asset in CACHED:
            meta = result.asset_materializations_for_node(asset)[0].metadata
            print(
                f"  {asset:<16} hits {meta['cache_hits'].value:>5}  misses {meta['cache_misses'].value:>5}",
                flush=True,
            )
        print(f"  {'total time':<16} {time.perf_counter() - started:.2f}s", flush=True)


if __name__ == "__main__":
    main()
