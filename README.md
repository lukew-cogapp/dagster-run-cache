# dagster-run-cache

A Redis-style key-value cache for Dagster that persists between runs.

Assets call `get`, `set`, `has` and friends on a `RunCache` resource. Entries
are files under a directory, one per key, so the cache survives one container
per run and works on a shared network mount with nothing to lock.

## API

```python
from dagster_run_cache import RunCache, content_key

cache.get("geocode:Tokyo", default=None)
cache.set("geocode:Tokyo", (35.7, 139.7), ttl=timedelta(days=30))
cache.has("geocode:Tokyo")
cache.delete("geocode:Tokyo")
cache.add("geocode:Tokyo", value)              # only if absent
cache.get_or_set("geocode:Tokyo", lambda: geocode("Tokyo"))
cache.get_many(keys)                           # hits only
cache.set_many({key: value, ...})
cache.clear("geocode")                         # every key under the prefix
cache.take_stats()                             # {"cache_hits": n, "cache_misses": n}, then reset
```

Register it once in `Definitions` and request it by name in any asset:

```python
defs = dg.Definitions(assets=[...], resources={"cache": RunCache(base_dir="output/cache")})

@dg.asset
def place_geocodes(cache: RunCache, documents: pl.LazyFrame) -> pl.DataFrame: ...
```

### Keys

Keys follow the Redis `prefix:rest` convention. The prefix is a directory on
disk, so `clear("embed")` drops every embedding and leaves the rest.

A short natural key reads best where one exists: `f"geocode:{place}"`.
Where the inputs are long or several, `content_key` hashes them:

```python
content_key("embed", model, text)   # "embed:9f2c…"
```

Any change to any part gives a new key, so invalidation needs no code: a
retitled document or a new model simply misses.

### Values

Values are pickled and zlib-compressed, as Django's file cache does, so any
Python value works: vectors, tuples, dicts holding dates. Two consequences:

- Only the pipeline should be able to write to `base_dir`. Loading a pickle
  can run code.
- A class must still be importable when its value is read. An entry that no
  longer unpickles reads as a miss and is recomputed.

## Demo

Three assets each show one usage pattern against the same fake source
(`src/dagster_run_cache/defs.py`). The services in `fakes.py` sleep to stand
in for latency.

| Asset | Pattern | Key |
|---|---|---|
| `place_geocodes` | `get_or_set` per item | `geocode:{place}` |
| `doc_embeddings` | `get_many`, batch the misses to the endpoint, `set_many` | `content_key("embed", model, text)` |
| `image_analysis` | `get`, compute on a miss, `set` with a TTL | `content_key("image", file_name, file_date)` |

```sh
uv sync
uv run python scripts/demo.py
```

Four runs, each on a fresh ephemeral Dagster instance, so only the cache
directory carries over:

```
Run 1: first run, empty cache…
  place_geocodes   hits     0  misses    12
  doc_embeddings   hits     0  misses  1000
  image_analysis   hits     0  misses  1000
  total time       9.53s

Run 2: nothing changed…
  place_geocodes   hits    12  misses     0
  doc_embeddings   hits  1000  misses     0
  image_analysis   hits  1000  misses     0
  total time       0.23s

Run 3: source edited: 10 retitled, 3 re-photographed, 5 deleted, 5 added in a new place…
  place_geocodes   hits    12  misses     1
  doc_embeddings   hits   985  misses    15
  image_analysis   hits   992  misses     8
  total time       0.34s

Run 4: embedding model bumped…
  place_geocodes   hits    13  misses     0
  doc_embeddings   hits     0  misses  1000
  image_analysis   hits  1000  misses     0
  total time       2.65s
```

To browse the assets in the Dagster UI instead:

```sh
uv run dagster dev -m dagster_run_cache.defs
```

## Limits

- **Stale keys stay** until `clear()` or their TTL. An old model's
  embeddings or a retitled document's old vector take disk space but are never
  read again.
- **One file per key.** A cold run over 150K keys writes 150K small files:
  fine on local disk, slower on a network mount. Later runs only write what
  changed.
- **`add` is not atomic** across concurrent runs; both may store.
- **Rename atomicity on the mount.** Writes rely on `os.replace` being atomic,
  which holds on local filesystems and NFS. Check it on any other mount type.
- Polars is pinned below 2.0 until dagster-polars supports it.

## Why not a library

`diskcache` has nearly this API but keeps its index in SQLite, whose locking
is unreliable on NFS. `dogpile.cache`'s file backend is dbm, with the same
problem, and its region setup adds more than it saves. `cachetools` is
in-memory only. A Redis server would need running; if one appears, `redis-py`
can sit behind the same `RunCache` methods without changing any asset.

## Development

```sh
uv run ruff format . && uv run ruff check . && uv run mypy src && uv run pytest
lefthook install
```
