"""Run the pipeline four times against one storage dir and print what the cache saved for each asset.

Each run uses a fresh ephemeral Dagster instance, so the only thing carried
between runs is the cache directory on disk, as with a container-per-run deployment.

    uv run python scripts/demo.py
"""

import shutil
import time
from pathlib import Path

from dagster_run_cache.defs import run_demo

STORAGE = Path(__file__).resolve().parent.parent / "output"

RUNS = [
    ("first run, empty cache", 1, "fake-embed-v1"),
    ("nothing changed", 1, "fake-embed-v1"),
    ("source edited: 10 retitled, 3 re-photographed, 5 deleted, 5 added in a new place", 2, "fake-embed-v1"),
    ("embedding model bumped", 2, "fake-embed-v2"),
]


def main() -> None:
    """Wipe the storage dir, then materialise once per entry in ``RUNS``."""
    shutil.rmtree(STORAGE, ignore_errors=True)
    for n, (label, edition, model) in enumerate(RUNS, start=1):
        print(f"\nRun {n}: {label}…", flush=True)
        started = time.perf_counter()
        for asset, (hits, misses) in run_demo(STORAGE, edition=edition, model=model).items():
            print(f"  {asset:<21} hits {hits:>5}  misses {misses:>5}", flush=True)
        print(f"  {'total time':<21} {time.perf_counter() - started:.2f}s", flush=True)


if __name__ == "__main__":
    main()
