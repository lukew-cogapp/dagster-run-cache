# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`RunCache`, a Dagster resource that caches computed rows between runs: one
Parquet file per prefix, keyed by one or more columns, with `compute`,
`missing`, `store`, `fetch`, `clear` and `take_stats`. Plus a demo pipeline
that exercises it. The README covers the API, key choice and limits; read it
first.

## Commands

```sh
uv sync
uv run python scripts/demo.py                    # four runs, prints hits/misses per asset
uv run dagster dev -m dagster_run_cache.defs     # UI (the user runs this, not Claude)

uv run ruff format . && uv run ruff check . && uv run mypy src && uv run pytest   # the CI gate
uv run pytest tests/test_cache.py::test_multi_column_key                          # one test
```

`ruff format` also formats Python code blocks inside `README.md`, and CI checks
that. The lefthook format job globs `*.{py,md}` for this reason; run the
formatter after any README edit.

After renaming or moving the repo directory, `rm -rf .venv && uv sync`: the
venv's scripts hold absolute paths.

## Layout

- `src/dagster_run_cache/utils/cache.py`: the reusable part. No demo code
  belongs under `utils/`. The package root re-exports `RunCache`.
- `src/dagster_run_cache/fakes.py`: fake source (`fake_documents(edition, size)`)
  and fake slow endpoints that sleep to stand in for latency.
- `src/dagster_run_cache/defs.py`: the three demo assets, the `Definitions`,
  and `run_demo`, which both `scripts/demo.py` and `tests/test_demo.py` call.

## Design decisions the user made

These were argued through; don't reopen them without a new reason.

- **Table only:** an earlier Redis-style per-key tier (`get`/`set`, a pickle
  file per key) was removed. Pipeline work arrives as frames, so a join beats a
  file read per key, and typed columns beat pickles.
- **One file per prefix**, at `<base_dir>/<prefix>.parquet`, no subfolders.
  Not a directory of delta files: the user rejected that even though it avoids
  the rewrite.
- **Keys are columns, not hashes.** No `content_key`; multi-column keys
  (`["model", "text"]`) carry every input that changes the result. No TTL:
  a refresh trigger goes in the key.

## How it works

- `store` collects the new rows (deduped on key, last wins), anti-joins the
  old file against them, streams both into a temp file with `sink_parquet`, then
  `os.replace`s it (`_atomic_write`). A changed column schema raises and points
  at `clear`. Concurrent stores are last-writer-wins: lost rows cost a
  recompute.
- `missing` counts hits and misses on the resource (`PrivateAttr`); `fetch`
  and `store` do not. Each demo asset ends with
  `context.add_output_metadata(cache.take_stats())`, and `run_demo` reads
  `cache_hits` / `cache_misses` back from that metadata.
- `fetch` on a prefix with no file returns `frame.head(0)`, so `compute` on an
  empty first run returns empty rather than raising.

## Demo scenario numbers

`fake_documents` edition 2 retitles 10 documents, re-photographs 3, deletes 5,
and adds 5 sharing one new place. The tests assert the resulting hit/miss
counts per asset; changing the edition logic means updating `tests/test_demo.py`
and the README output block together.

## Constraints

- Python 3.14 (`.python-version`); PEP 695 `type` aliases are used.
- Polars is pinned `<2`: dagster-polars still passes `rechunk` to
  `scan_parquet`, which Polars 2.0 removed. Lift the pin once dagster-polars
  supports Polars 2.
- mypy runs strict; `dg.ConfigurableResource` needs a
  `# type: ignore[type-arg]` on subclasses.
- Ruff ignores D417 on purpose: `Args` entries only where the name and type are
  not enough.
- `output/` holds Parquet and the demo cache; it is gitignored and the demo
  script wipes it on each run.
