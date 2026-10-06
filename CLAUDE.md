# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`RunCache`, a Dagster resource that caches computed rows between runs: one
Parquet file per table, keyed by one or more columns, with `compute`,
`lookup`, `store`, `fetch` and `clear`. Plus a demo pipeline
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
- **One file per table**, at `<base_dir>/<table>.parquet`, no subfolders.
  Not a directory of delta files: the user rejected that even though it avoids
  the rewrite.
- **Keys are columns, not hashes.** No `content_key`; multi-column keys
  (`["model", "text"]`) carry every input that changes the result. No TTL:
  a refresh trigger goes in the key.
- **Stats come back from each call**, not from counters on the resource:
  `lookup` returns a `Lookup` whose `.metadata` holds `cache/<table>/hits` and
  `cache/<table>/misses`, and `compute` returns `(LazyFrame, Lookup)`.
- **Called "table", not "prefix"**, so it is not confused with a Dagster
  asset-key prefix.
- **Not yet done, by choice:** S3/UPath paths, an asset-check factory and
  Pandera schemas. They block upstreaming into collection-flow, not this repo;
  FAMSF reads S3 through an NFS mount.

## How it works

- `lookup` stays lazy until one `pl.collect_all` that returns the misses, the
  distinct-key count and the key null counts together, so a lazy input plan
  runs once. Hit and miss counts are distinct keys, not rows.
- `compute` passes `fn` only the distinct uncached keys, checks the result
  (key columns present, every key returned, no column clashing with the input
  frame), and stores each `batch_size` slice as it finishes.
- `store` dedupes the new rows on key (last wins), rejects null keys, aligns
  them to the stored schema (column order, then a strict cast), anti-joins the
  old file against them, streams both into a temp file with `sink_parquet`,
  then `os.replace`s it (`_atomic_write`). Different column names or a failed
  cast raise and point at `clear`. Concurrent stores are last-writer-wins.
- `fetch` returns a `LazyFrame`; on a table with no file it returns
  `frame.head(0)`, so `compute` on an empty first run returns empty.
- Demo assets return `LazyFrame` for the IO manager to sink. `run_demo` maps
  each asset to its table (`CACHE_TABLES`) to read the counts back.

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
