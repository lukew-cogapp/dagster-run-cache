# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A `Cache` interface for Dagster resources that persists between runs, with
backends chosen per cache (`ParquetCache`, `MemoryCache`), and a demo pipeline
whose service resources hold a cache. The README covers the API, key choice and
limits; read it first.

## Commands

```sh
uv sync
uv run python scripts/demo.py                    # four runs, prints hits/misses per asset
uv run dg dev                                    # UI (the user runs this, not Claude)

uv run ruff format . && uv run ruff check . && uv run mypy src && uv run pytest   # the CI gate
uv run pytest tests/test_cache.py -k multi_column                                 # one test, both backends
```

`ruff format` also formats Python code blocks inside `README.md`, and CI checks
that. The lefthook format job globs `*.{py,md}` for this reason; run the
formatter after any README edit.

After renaming or moving the repo directory, `rm -rf .venv && uv sync`: the
venv's scripts hold absolute paths.

## Layout

- `src/dagster_run_cache/utils/cache.py`: the `Cache` base resource, its
  default frame calls (built on `get_many` / `set_many`), `MemoryCache`, and
  `Lookup` / `cache_metadata`.
- `src/dagster_run_cache/utils/parquet_cache.py`: `ParquetCache`, which
  overrides the frame calls with native joins, plus `find_misses`,
  `merge_rows` and `_atomic_write`.
- No demo code belongs under `utils/`. The package root re-exports `Cache`,
  `Lookup`, `MemoryCache` and `ParquetCache`.
- `src/dagster_run_cache/fakes.py`: fake source (`fake_documents(edition, size)`)
  and fake slow endpoints that sleep to stand in for latency.
- `src/dagster_run_cache/defs.py`: the `Geocoder` and `Embedder` resources, the
  four demo assets, `resources(storage, model)` (shared by `Definitions` and
  `run_demo`), and `run_demo`, which `scripts/demo.py` and `tests/test_demo.py`
  call.
- `tests/test_cache.py` runs one contract suite against both backends through a
  parametrised fixture, so they cannot drift; backend-specific behaviour has
  its own `test_parquet_cache_*` tests.

## Design decisions the user made

These were argued through; don't reopen them without a new reason.

- **A `Cache` interface injected into service resources**, after a
  collection-flow maintainer's suggestion: `Embedder(cache=ParquetCache(...))`,
  backend chosen in the wiring. `Cache` is an abstract `ConfigurableResource`
  rather than a `typing.Protocol`, since Dagster types nested resource fields
  by resource class; a field typed `Cache` accepts any subclass (checked).
- **`get_many` / `set_many` are part of the interface.** The nightly rebuild
  needs a value for all ~150K documents every run, so per-key round trips are
  the cost to avoid; the frame calls exist for the same reason.
- **One cache instance is one namespace** (`name`), at
  `<base_dir>/<name>.parquet` for `ParquetCache`. No subfolders, and not a
  directory of delta files: the user rejected that even though it avoids the
  rewrite.
- **Keys are columns, not hashes.** Multi-column keys (`["model", "text"]`)
  carry every input that changes the result. No TTL: a refresh trigger goes in
  the key.
- **No pickles.** `ParquetCache` stores Polars-representable values;
  `MemoryCache` holds Python objects in memory only.
- **Stats come back from each call**, as a `Lookup` whose `.metadata` holds
  `cache/<name>/hits` and `cache/<name>/misses`; no counters on the resource.
- **Removed, and why:** a Redis-style per-key pickle store (one file per key,
  slow at volume); a `CachedParquetIOManager` (one asset per cache, unreachable
  from resources, no per-batch saves). Redis is the next backend, not built
  (no server to test against).
- **Not yet done, by choice:** S3/UPath paths, an asset-check factory and
  Pandera schemas. They block upstreaming into collection-flow, not this repo;
  FAMSF reads S3 through an NFS mount.

## How it works

- Base `Cache` frame calls join key columns into one string (`\x1f`-separated)
  for `get_many` / `set_many`, storing non-key columns as a dict per row.
  `ParquetCache` overrides `lookup` / `store` / `fetch` to work on the columns
  directly, and implements its per-key calls as frame calls on a `key` and a
  `value` column, so one cache should use one style.
- `lookup` (Parquet) stays lazy until one `pl.collect_all` returning the misses,
  the distinct-key count and the key null counts. Counts are distinct keys.
- `compute` passes `fn` only the distinct uncached keys, checks the result (key
  columns present, every key returned, no column clashing with the input), and
  stores each `batch_size` slice as it finishes.
- `ParquetCache.store` dedupes on key (last wins), rejects null keys, aligns new
  rows to the stored schema (column order, then a strict cast), anti-joins the
  old file, streams both into a temp file with `sink_parquet`, then
  `os.replace`s it. Concurrent stores are last-writer-wins.
- `fetch` returns a `LazyFrame`; on an empty cache it returns `frame.head(0)`.
- In the demo, `doc_embeddings` keys on the text through `Embedder.embed`;
  `doc_embeddings_by_id` keys on `doc_id` + `modified` and calls the three
  steps itself, because its endpoint needs the text, which is not in the key.

## Demo scenario numbers

`fake_documents` edition 2 retitles 10 documents (bumping `modified`),
re-photographs 3 (bumping `file_date`), deletes 5, and adds 5 sharing one new
place. The tests assert the resulting hit/miss counts per asset; changing the
edition logic means updating `tests/test_demo.py` and the README output block
together.

## Constraints

- Python 3.14 (`.python-version`); PEP 695 `type` aliases are used.
- `Cache` defines a method named `set`, which shadows the builtin inside the
  class body: annotate with `frozenset` or `builtins.set` there.
- Polars is pinned `<2`: dagster-polars still passes `rechunk` to
  `scan_parquet`, which Polars 2.0 removed. Lift the pin once dagster-polars
  supports Polars 2.
- mypy runs strict; `dg.ConfigurableResource` needs a
  `# type: ignore[type-arg]` on direct subclasses.
- Ruff ignores D417 on purpose: `Args` entries only where the name and type are
  not enough.
- `output/` holds Parquet and the demo cache; it is gitignored and the demo
  script wipes it on each run.
