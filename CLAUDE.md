# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`RunCache`, a Dagster resource that persists between Dagster runs, with two
tiers: a Redis-style per-key API (`get`, `set`, `has`, `delete`, `add`,
`get_or_set`, `get_many`, `set_many`) and a Parquet table API for bulk work
(`missing`, `store`, `fetch`). Both share `clear` and `take_stats`. Plus a demo
pipeline that exercises it. The README covers the API, key convention and
limits; read it first.

## Commands

```sh
uv sync
uv run python scripts/demo.py                    # four runs, prints hits/misses per asset
uv run dagster dev -m dagster_run_cache.defs     # UI (the user runs this, not Claude)

uv run ruff format . && uv run ruff check . && uv run mypy src && uv run pytest   # the CI gate
uv run pytest tests/test_cache.py::test_ttl_expires_entries                       # one test
```

`ruff format` also formats Python code blocks inside `README.md`, and CI checks
that. The lefthook format job globs `*.{py,md}` for this reason; run the
formatter after any README edit.

## Layout

- `src/dagster_run_cache/utils/cache.py`: the reusable part. No demo code
  belongs under `utils/`. The package root re-exports `RunCache` and
  `content_key`.
- `src/dagster_run_cache/fakes.py`: fake source (`fake_documents(edition, size)`)
  and fake slow endpoints that sleep to stand in for latency.
- `src/dagster_run_cache/defs.py`: three assets, each showing one usage
  pattern (per-key `get_or_set`, the table tier with batched misses, plain
  `get`/`set` with a TTL), and the `Definitions`.
- `tests/test_cache.py` unit-tests the resource; `tests/test_demo.py` runs the
  four-run scenario through `dg.materialize` and asserts hit/miss counts.

## How the cache works

Per-key tier:

- One file per key at `<base_dir>/<prefix>/<sha256[:2]>/<sha256>.pkl`, where
  `prefix` is the part of the key before the first `:` (`_` if none). No shared
  index or database, so it is safe on an NFS mount with concurrent runs. Keep it
  that way: a SQLite- or dbm-backed store (diskcache, dogpile's dbm) was
  rejected for NFS locking.
- File format: a pickled `expires_at` (float or `None`) followed by the
  zlib-compressed pickled value. `has` reads only the first pickle. Writes go to
  a temp file in the same directory, then `os.replace`.
- Any unreadable entry (missing, expired, corrupt, class moved) is a miss, never
  an error.

Table tier:

- One Parquet file per prefix at `<base_dir>/<prefix>/table.parquet`, inside
  the prefix directory so `clear(prefix)` removes it with the per-key entries.
- A Parquet file cannot be appended to, so `store` streams the old rows (minus
  replaced keys) and the new rows into a temp file with `sink_parquet`, then
  `os.replace`s it. The user wants a single file per prefix, not a directory
  of delta files; don't reintroduce one.
- Concurrent stores are last-writer-wins: the loser's new rows are dropped,
  costing a recompute. A changed column schema raises and points at `clear`.

Both tiers:

- Invalidation is by key, not by version flags: `content_key(prefix, *parts)`
  hashes the inputs, so changed inputs produce a new key and old entries go
  stale until `clear(prefix)` or TTL.
- Hit/miss counters live on the resource (`PrivateAttr`). Each demo asset calls
  `context.add_output_metadata(cache.take_stats())` at the end, and the demo and
  tests read `cache_hits` / `cache_misses` from that metadata. `get`-based reads
  and `missing` count; `has` and `fetch` do not.
- Time goes through `utils.cache._now()` so tests can monkeypatch the clock.

## Demo scenario numbers

`fake_documents` edition 2 retitles 10 documents, re-photographs 3, deletes 5,
and adds 5 sharing one new place. The tests assert the resulting hit/miss
counts per asset; changing the edition logic means updating `tests/test_demo.py`
and the README output block together.

## Constraints

- Python 3.14 (`.python-version`); PEP 695 generics are used (`get_or_set[T]`).
- Polars is pinned `<2`: dagster-polars still passes `rechunk` to
  `scan_parquet`, which Polars 2.0 removed. Lift the pin once dagster-polars
  supports Polars 2.
- mypy runs strict; `dg.ConfigurableResource` needs a
  `# type: ignore[type-arg]` on subclasses.
- Ruff ignores D417 on purpose: `Args` entries only where the name and type are
  not enough.
- `output/` holds Parquet and the demo cache; it is gitignored and the demo
  script wipes it on each run.
