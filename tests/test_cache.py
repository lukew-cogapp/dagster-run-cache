from pathlib import Path

import polars as pl
import pytest

from dagster_run_cache import Cache, MemoryCache, ParquetCache


@pytest.fixture(params=["memory", "parquet"])
def cache(request: pytest.FixtureRequest, tmp_path: Path) -> Cache:
    """Return each backend in turn, so both meet the same contract."""
    if request.param == "memory":
        return MemoryCache(name="thing")
    return ParquetCache(name="thing", base_dir=str(tmp_path))


def _rows(*pairs: tuple[str, int]) -> pl.DataFrame:
    return pl.DataFrame({"k": [p[0] for p in pairs], "v": [p[1] for p in pairs]})


def _length(batch: pl.DataFrame) -> pl.DataFrame:
    return batch.with_columns(v=pl.col("k").str.len_chars())


def test_get_set_has_delete(cache: Cache) -> None:
    """Per-key calls round-trip a value and report presence."""
    assert cache.get("a") is None
    cache.set("a", [35.7, 139.7])
    assert cache.get("a") == [35.7, 139.7]
    assert cache.has("a")
    assert cache.delete("a")
    assert not cache.has("a")
    assert not cache.delete("a")


def test_get_many_returns_hits_only(cache: Cache) -> None:
    """Misses are left out of ``get_many`` rather than mapped to None."""
    cache.set_many({"a": 1, "b": 2})
    assert cache.get_many(["a", "b", "c"]) == {"a": 1, "b": 2}


def test_set_replaces_an_existing_value(cache: Cache) -> None:
    """Setting a cached key replaces its value."""
    cache.set("a", 1)
    cache.set("a", 2)
    assert cache.get("a") == 2


def test_clear_counts_and_empties(cache: Cache) -> None:
    """``clear`` removes everything and reports how many entries there were."""
    cache.set_many({"a": 1, "b": 2})
    assert cache.clear() == 2
    assert cache.clear() == 0
    assert cache.get_many(["a", "b"]) == {}


def test_lookup_on_empty_cache_misses_every_row(cache: Cache) -> None:
    """With nothing stored yet, every row is a miss."""
    frame = pl.DataFrame({"k": ["a", "b"]})
    lookup = cache.lookup(frame, key="k")
    assert lookup.misses.equals(frame)
    assert (lookup.hit_count, lookup.miss_count) == (0, 2)


def test_store_then_lookup_misses_only_new_rows(cache: Cache) -> None:
    """Stored keys stop being misses; unstored ones remain."""
    cache.store(_rows(("a", 1), ("b", 2)), key="k")
    assert cache.lookup(pl.DataFrame({"k": ["a", "b", "c"]}), key="k").misses["k"].to_list() == ["c"]


def test_lookup_counts_distinct_keys_not_rows(cache: Cache) -> None:
    """Five rows sharing a key count as one miss, since ``fn`` would run once."""
    lookup = cache.lookup(pl.DataFrame({"k": ["a"] * 5}), key="k")
    assert (lookup.hit_count, lookup.miss_count) == (0, 1)


def test_lookup_metadata_is_namespaced_by_cache(cache: Cache) -> None:
    """Metadata keys carry the cache name, so one asset can report several caches."""
    cache.store(_rows(("a", 1)), key="k")
    metadata = cache.lookup(pl.DataFrame({"k": ["a", "b"]}), key="k").metadata
    assert {k: v.value for k, v in metadata.items()} == {"cache/thing/hits": 1, "cache/thing/misses": 1}


def test_fetch_joins_stored_columns(cache: Cache) -> None:
    """``fetch`` returns the frame with the stored columns joined on; uncached rows drop out."""
    cache.store(_rows(("a", 1), ("b", 2)), key="k")
    fetched = cache.fetch(pl.DataFrame({"k": ["b", "a", "z"]}), key="k").collect()
    assert fetched.sort("k").to_dicts() == [{"k": "a", "v": 1}, {"k": "b", "v": 2}]


def test_fetch_on_empty_cache_returns_no_rows(cache: Cache) -> None:
    """Fetching before anything is stored gives an empty frame rather than an error."""
    assert cache.fetch(pl.DataFrame({"k": ["a"]}), key="k").collect().is_empty()


def test_store_replaces_existing_keys_and_keeps_the_rest(cache: Cache) -> None:
    """A second store overwrites shared keys, adds new ones and leaves the others."""
    cache.store(_rows(("a", 1), ("b", 2)), key="k")
    cache.store(_rows(("b", 20), ("c", 3)), key="k")
    fetched = cache.fetch(pl.DataFrame({"k": ["a", "b", "c"]}), key="k").collect().sort("k")
    assert fetched["v"].to_list() == [1, 20, 3]


def test_multi_column_key(cache: Cache) -> None:
    """A key of several columns matches only when every column matches."""
    cache.store(pl.DataFrame({"name": ["a"], "date": [1], "v": [10]}), key=["name", "date"])
    frame = pl.DataFrame({"name": ["a", "a"], "date": [1, 2]})
    assert cache.lookup(frame, key=["name", "date"]).misses["date"].to_list() == [2]


def test_null_keys_are_rejected(cache: Cache) -> None:
    """A null key raises, since joins would never match it."""
    with pytest.raises(ValueError, match="nulls"):
        cache.lookup(pl.DataFrame({"k": ["a", None]}), key="k")
    with pytest.raises(ValueError, match="nulls"):
        cache.store(pl.DataFrame({"k": [None], "v": [1]}, schema={"k": pl.String, "v": pl.Int64}), key="k")


def test_key_missing_an_input_does_not_see_its_changes(cache: Cache) -> None:
    """A key that leaves out an input (here the text) hits on an edited row and returns its old value."""
    cache.store(pl.DataFrame({"id": [1], "v": ["old"]}), key="id")
    edited = pl.DataFrame({"id": [1], "text": ["new"]})
    assert cache.lookup(edited, key="id").miss_count == 0
    assert cache.fetch(edited, key="id").collect()["v"].to_list() == ["old"]


def test_compute_calls_fn_once_per_uncached_key(cache: Cache) -> None:
    """``compute`` sends only uncached keys to ``fn``, once each, and returns every row with its value."""
    seen: list[list[str]] = []

    def fn(batch: pl.DataFrame) -> pl.DataFrame:
        seen.append(batch["k"].to_list())
        return _length(batch)

    first, _ = cache.compute(pl.DataFrame({"k": ["a", "bb", "a"]}), key="k", fn=fn)
    second, lookup = cache.compute(pl.DataFrame({"k": ["a", "ccc"]}), key="k", fn=fn)

    assert seen == [["a", "bb"], ["ccc"]]
    assert first.collect().sort("k")["v"].to_list() == [1, 1, 2]
    assert second.collect().sort("k")["v"].to_list() == [1, 3]
    assert (lookup.hit_count, lookup.miss_count) == (1, 1)


def test_compute_passes_fn_only_the_key_columns(cache: Cache) -> None:
    """Non-key columns stay out of ``fn``, so they can neither leak into the cache nor clash."""
    seen: list[list[str]] = []

    def fn(batch: pl.DataFrame) -> pl.DataFrame:
        seen.append(batch.columns)
        return _length(batch)

    cache.compute(pl.DataFrame({"k": ["a"], "doc": [1]}), key="k", fn=fn)
    assert seen == [["k"]]


def test_compute_rejects_fn_dropping_keys(cache: Cache) -> None:
    """``fn`` returning fewer keys than it was given raises, naming how many."""
    with pytest.raises(ValueError, match="no row for 1 of 2 keys"):
        cache.compute(pl.DataFrame({"k": ["a", "b"]}), key="k", fn=lambda b: _length(b.head(1)))


def test_compute_rejects_fn_columns_clashing_with_the_frame(cache: Cache) -> None:
    """A cached column named like an input column raises rather than producing ``_right`` columns."""
    with pytest.raises(ValueError, match="already has"):
        cache.compute(pl.DataFrame({"k": ["a"], "v": [0]}), key="k", fn=lambda b: b.with_columns(v=pl.lit(1)))


def test_compute_keeps_finished_batches_when_a_later_one_fails(cache: Cache) -> None:
    """With ``batch_size``, batches stored before a failure stay cached for the next run."""
    calls = []

    def fn(batch: pl.DataFrame) -> pl.DataFrame:
        calls.append(batch.height)
        if len(calls) == 2:
            raise RuntimeError("endpoint down")
        return _length(batch)

    frame = pl.DataFrame({"k": ["a", "b", "c", "d"]})
    with pytest.raises(RuntimeError):
        cache.compute(frame, key="k", fn=fn, batch_size=2)
    assert cache.lookup(frame, key="k").miss_count == 2


def test_parquet_cache_persists_across_instances(tmp_path: Path) -> None:
    """A second instance on the same file, as in the next run, sees what the first stored."""
    ParquetCache(name="thing", base_dir=str(tmp_path)).set("a", 1)
    assert ParquetCache(name="thing", base_dir=str(tmp_path)).get("a") == 1


def test_parquet_cache_keeps_one_file_and_no_temp(tmp_path: Path) -> None:
    """Repeated stores leave a single file with no temp files beside it."""
    cache = ParquetCache(name="thing", base_dir=str(tmp_path))
    cache.store(_rows(("a", 1)), key="k")
    cache.store(_rows(("b", 2)), key="k")
    assert [p.name for p in tmp_path.iterdir()] == ["thing.parquet"]


def test_parquet_cache_aligns_column_order_and_compatible_dtypes(tmp_path: Path) -> None:
    """Reordered columns and a narrower integer type fit the stored table rather than raising."""
    cache = ParquetCache(name="thing", base_dir=str(tmp_path))
    cache.store(_rows(("a", 1)), key="k")
    cache.store(pl.DataFrame({"v": [2], "k": ["b"]}, schema={"v": pl.Int32, "k": pl.String}), key="k")
    stored = pl.read_parquet(cache.path)
    assert stored.schema == pl.Schema({"k": pl.String, "v": pl.Int64})
    assert stored.height == 2


def test_parquet_cache_rejects_changed_columns_and_uncastable_values(tmp_path: Path) -> None:
    """Different column names, or values that will not cast, raise rather than mixing shapes."""
    cache = ParquetCache(name="thing", base_dir=str(tmp_path))
    cache.store(_rows(("a", 1)), key="k")
    with pytest.raises(ValueError, match="clear"):
        cache.store(pl.DataFrame({"k": ["b"], "other": [1.0]}), key="k")
    with pytest.raises(ValueError, match="thing"):
        cache.store(pl.DataFrame({"k": ["b"], "v": ["not a number"]}), key="k")


def test_parquet_cache_rejects_unsafe_names(tmp_path: Path) -> None:
    """A name that could escape ``base_dir`` raises."""
    with pytest.raises(ValueError):
        _ = ParquetCache(name="../escape", base_dir=str(tmp_path)).path
