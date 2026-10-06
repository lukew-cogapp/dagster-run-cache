# dagster-cached-compute-demo

A helper that lets a Dagster asset recompute only the rows whose inputs changed
since its last materialisation. Built for expensive per-row work such as
embedding text or fetching documents.

The cache is the asset's own previous output, read back through its IO manager
with `context.load_asset_value`. There is no separate cache store, so it works
with one container per run and needs nothing beyond the Parquet the asset
already writes.

## How it works

`cached_compute` (`src/cached_compute_demo/cached_compute.py`):

1. Hashes each row's input columns plus a `version` string (polars-hash wyhash,
   stable across Polars versions).
2. Loads the asset's previous output. A first run has none.
3. Keeps prior rows whose key and hash still match the current input.
4. Calls `compute` in batches on the rest only.
5. Returns kept and fresh rows together. Keys missing from the input drop out.

Bump `version` to invalidate everything (a new model, a logic change).

```python
@dg.asset
def doc_embeddings(context, documents: pl.LazyFrame):
    return cached_compute(
        context,
        documents,
        key=["doc_id"],
        inputs=["title"],
        compute=lambda batch: batch.select("doc_id", embed(batch["title"])),
        version="model-v1",
    )
```

## Demo

```sh
uv sync
uv run python scripts/demo.py
```

Four runs against one `output/` dir, each with a fresh ephemeral Dagster
instance, so only the Parquet on disk carries over:

```
Run 1: first run, empty cache…
  hits     0  misses  1000  rows  1000   2.51s
Run 2: nothing changed…
  hits  1000  misses     0  rows  1000   0.15s
Run 3: source edited: 10 retitled, 5 deleted, 5 added…
  hits   985  misses    15  rows  1000   0.17s
Run 4: embedding model bumped…
  hits     0  misses  1000  rows  1000   2.16s
```

## Limits

- The result is collected, not returned lazy. The kept rows are read from the
  file the IO manager is about to overwrite, so they must be in memory first.
  Peak memory is the size of the asset's output.
- Misses are collected with their input columns. Keep `inputs` to the columns
  that matter (the text to embed, not the whole document).
- Results must fit Parquet columns. Vectors go in as `pl.Array(pl.Float32, n)`.
- Each asset caches its own output. Two assets embedding the same text pay
  twice; make the embeddings their own asset instead.
- Polars is pinned below 2.0 until dagster-polars supports it.

## Development

```sh
uv run ruff format . && uv run ruff check . && uv run mypy src && uv run pytest
lefthook install
```
