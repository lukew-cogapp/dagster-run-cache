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

Three assets share the one helper, each caching a different kind of expensive
work against the same fake source (`src/cached_compute_demo/defs.py`):

| Asset | Expensive call | Key | Recomputed when |
|---|---|---|---|
| `doc_embeddings` | batched embedding endpoint | `doc_id` | title, artist or medium changes, or the model version is bumped |
| `place_geocodes` | geocoder, one call per place | place string | a new place appears; places shared by many documents are looked up once |
| `image_analysis` | image decode | `file_name` | the file's `file_date` moves on (re-photographed) |

The services are fakes in `fakes.py` that sleep to stand in for latency.

```sh
uv sync
uv run python scripts/demo.py
```

Four runs against one `output/` dir, each with a fresh ephemeral Dagster
instance, so only the Parquet on disk carries over:

```
Run 1: first run, empty cache…
  doc_embeddings   hits     0  misses  1000
  place_geocodes   hits     0  misses    12
  image_analysis   hits     0  misses  1000
  total time       7.46s

Run 2: nothing changed…
  doc_embeddings   hits  1000  misses     0
  place_geocodes   hits    12  misses     0
  image_analysis   hits  1000  misses     0
  total time       0.30s

Run 3: source edited: 10 retitled, 3 re-photographed, 5 deleted, 5 added in a new place…
  doc_embeddings   hits   985  misses    15
  place_geocodes   hits    12  misses     1
  image_analysis   hits   992  misses     8
  total time       0.42s

Run 4: embedding model bumped…
  doc_embeddings   hits     0  misses  1000
  place_geocodes   hits    13  misses     0
  image_analysis   hits  1000  misses     0
  total time       2.37s
```

To browse the assets in the Dagster UI instead:

```sh
uv run dagster dev -m cached_compute_demo.defs
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
