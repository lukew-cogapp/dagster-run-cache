from pathlib import Path

import polars as pl
import pytest

from dagster_run_cache import RunCache


@pytest.fixture
def cache(tmp_path: Path) -> RunCache:
    """Return a cache in a fresh directory."""
    return RunCache(base_dir=str(tmp_path))


def _rows(*pairs: tuple[str, int]) -> pl.DataFrame:
    return pl.DataFrame({"k": [p[0] for p in pairs], "v": [p[1] for p in pairs]})


def _length(batch: pl.DataFrame) -> pl.DataFrame:
    return batch.with_columns(v=pl.col("k").str.len_chars())


def test_lookup_on_empty_cache_misses_every_row(cache: RunCache) -> None:
    """With nothing stored yet, every row is a miss."""
    frame = pl.DataFrame({"k": ["a", "b"]})
    lookup = cache.lookup("thing", frame, key="k")
    assert lookup.misses.equals(frame)
    assert (lookup.hit_count, lookup.miss_count) == (0, 2)


def test_store_then_lookup_misses_only_new_rows(cache: RunCache) -> None:
    """Stored keys stop being misses; unstored ones remain."""
    cache.store("thing", _rows(("a", 1), ("b", 2)), key="k")
    lookup = cache.lookup("thing", pl.DataFrame({"k": ["a", "b", "c"]}), key="k")
    assert lookup.misses["k"].to_list() == ["c"]


def test_lookup_counts_distinct_keys_not_rows(cache: RunCache) -> None:
    """Five rows sharing a key count as one miss, since ``fn`` would run once."""
    lookup = cache.lookup("thing", pl.DataFrame({"k": ["a"] * 5}), key="k")
    assert (lookup.hit_count, lookup.miss_count) == (0, 1)


def test_lookup_metadata_is_namespaced_by_table(cache: RunCache) -> None:
    """Metadata keys carry the table name, so one asset can report several tables."""
    cache.store("thing", _rows(("a", 1)), key="k")
    metadata = cache.lookup("thing", pl.DataFrame({"k": ["a", "b"]}), key="k").metadata
    assert {k: v.value for k, v in metadata.items()} == {"cache/thing/hits": 1, "cache/thing/misses": 1}


def test_fetch_joins_stored_columns(cache: RunCache) -> None:
    """``fetch`` returns the frame with the stored columns joined on."""
    cache.store("thing", _rows(("a", 1), ("b", 2)), key="k")
    fetched = cache.fetch("thing", pl.DataFrame({"k": ["b", "a"]}), key="k").collect()
    assert fetched.sort("k").to_dicts() == [{"k": "a", "v": 1}, {"k": "b", "v": 2}]


def test_fetch_on_empty_cache_returns_no_rows(cache: RunCache) -> None:
    """Fetching before anything is stored gives an empty frame rather than an error."""
    assert cache.fetch("thing", pl.DataFrame({"k": ["a"]}), key="k").collect().is_empty()


def test_store_replaces_existing_keys_and_keeps_the_rest(cache: RunCache, tmp_path: Path) -> None:
    """A second store overwrites shared keys, adds new ones and leaves the others."""
    cache.store("thing", _rows(("a", 1), ("b", 2)), key="k")
    cache.store("thing", _rows(("b", 20), ("c", 3)), key="k")
    stored = pl.read_parquet(tmp_path / "thing.parquet").sort("k")
    assert stored.to_dicts() == [{"k": "a", "v": 1}, {"k": "b", "v": 20}, {"k": "c", "v": 3}]


def test_store_keeps_last_of_duplicate_keys(cache: RunCache) -> None:
    """Duplicate keys within one store collapse to the last row."""
    cache.store("thing", _rows(("a", 1), ("a", 2)), key="k")
    assert cache.fetch("thing", pl.DataFrame({"k": ["a"]}), key="k").collect()["v"].to_list() == [2]


def test_store_aligns_column_order_and_compatible_dtypes(cache: RunCache, tmp_path: Path) -> None:
    """Reordered columns and a narrower integer type fit the stored table rather than raising."""
    cache.store("thing", _rows(("a", 1)), key="k")
    cache.store("thing", pl.DataFrame({"v": [2], "k": ["b"]}, schema={"v": pl.Int32, "k": pl.String}), key="k")
    stored = pl.read_parquet(tmp_path / "thing.parquet")
    assert stored.schema == pl.Schema({"k": pl.String, "v": pl.Int64})
    assert stored.height == 2


def test_store_rejects_changed_columns(cache: RunCache) -> None:
    """Storing different column names raises rather than mixing shapes in one file."""
    cache.store("thing", _rows(("a", 1)), key="k")
    with pytest.raises(ValueError, match="clear"):
        cache.store("thing", pl.DataFrame({"k": ["b"], "other": [1.0]}), key="k")


def test_store_rejects_dtypes_that_do_not_cast(cache: RunCache) -> None:
    """A value that cannot cast to the stored dtype raises, naming the table."""
    cache.store("thing", _rows(("a", 1)), key="k")
    with pytest.raises(ValueError, match="thing"):
        cache.store("thing", pl.DataFrame({"k": ["b"], "v": ["not a number"]}), key="k")


def test_null_keys_are_rejected(cache: RunCache) -> None:
    """A null key raises, since joins would never match it."""
    with pytest.raises(ValueError, match="nulls"):
        cache.lookup("thing", pl.DataFrame({"k": ["a", None]}), key="k")
    with pytest.raises(ValueError, match="nulls"):
        cache.store("thing", pl.DataFrame({"k": [None], "v": [1]}, schema={"k": pl.String, "v": pl.Int64}), key="k")


def test_multi_column_key(cache: RunCache) -> None:
    """A key of several columns matches only when every column matches."""
    cache.store("thing", pl.DataFrame({"name": ["a"], "date": [1], "v": [10]}), key=["name", "date"])
    frame = pl.DataFrame({"name": ["a", "a"], "date": [1, 2]})
    assert cache.lookup("thing", frame, key=["name", "date"]).misses["date"].to_list() == [2]


def test_store_leaves_one_file_and_no_temp(cache: RunCache, tmp_path: Path) -> None:
    """Repeated stores keep a single file per table with no temp files beside it."""
    cache.store("thing", _rows(("a", 1)), key="k")
    cache.store("thing", _rows(("b", 2)), key="k")
    assert [p.name for p in tmp_path.iterdir()] == ["thing.parquet"]


def test_clear_counts_and_removes_one_table(cache: RunCache) -> None:
    """``clear`` removes one table's rows and leaves other tables alone."""
    cache.store("thing", _rows(("a", 1), ("b", 2)), key="k")
    cache.store("other", _rows(("a", 1)), key="k")
    assert cache.clear("thing") == 2
    assert cache.clear("thing") == 0
    assert cache.lookup("other", pl.DataFrame({"k": ["a"]}), key="k").misses.is_empty()


def test_compute_calls_fn_once_per_uncached_key(cache: RunCache) -> None:
    """``compute`` sends only uncached keys to ``fn``, once each, and returns every row with its value."""
    seen: list[list[str]] = []

    def fn(batch: pl.DataFrame) -> pl.DataFrame:
        seen.append(batch["k"].to_list())
        return _length(batch)

    first, _ = cache.compute("thing", pl.DataFrame({"k": ["a", "bb", "a"]}), key="k", fn=fn)
    second, lookup = cache.compute("thing", pl.DataFrame({"k": ["a", "ccc"]}), key="k", fn=fn)

    assert seen == [["a", "bb"], ["ccc"]]
    assert first.collect().sort("k")["v"].to_list() == [1, 1, 2]
    assert second.collect().sort("k")["v"].to_list() == [1, 3]
    assert (lookup.hit_count, lookup.miss_count) == (1, 1)


def test_compute_passes_fn_only_the_key_columns(cache: RunCache) -> None:
    """Non-key columns stay out of ``fn``, so they can neither leak into the cache nor clash."""
    seen: list[list[str]] = []

    def fn(batch: pl.DataFrame) -> pl.DataFrame:
        seen.append(batch.columns)
        return _length(batch)

    cache.compute("thing", pl.DataFrame({"k": ["a"], "doc": [1]}), key="k", fn=fn)
    assert seen == [["k"]]


def test_compute_rejects_fn_dropping_keys(cache: RunCache) -> None:
    """``fn`` returning fewer keys than it was given raises, naming how many."""
    with pytest.raises(ValueError, match="no row for 1 of 2 keys"):
        cache.compute("thing", pl.DataFrame({"k": ["a", "b"]}), key="k", fn=lambda b: _length(b.head(1)))


def test_compute_rejects_fn_columns_clashing_with_the_frame(cache: RunCache) -> None:
    """A cached column named like an input column raises rather than producing ``_right`` columns."""
    with pytest.raises(ValueError, match="already has"):
        cache.compute("thing", pl.DataFrame({"k": ["a"], "v": [0]}), key="k", fn=lambda b: b.with_columns(v=pl.lit(1)))


def test_compute_keeps_finished_batches_when_a_later_one_fails(cache: RunCache) -> None:
    """With ``batch_size``, batches stored before a failure stay cached for the next run."""
    calls = []

    def fn(batch: pl.DataFrame) -> pl.DataFrame:
        calls.append(batch.height)
        if len(calls) == 2:
            raise RuntimeError("endpoint down")
        return _length(batch)

    with pytest.raises(RuntimeError):
        cache.compute("thing", pl.DataFrame({"k": ["a", "b", "c", "d"]}), key="k", fn=fn, batch_size=2)
    assert cache.lookup("thing", pl.DataFrame({"k": ["a", "b", "c", "d"]}), key="k").miss_count == 2


def test_unsafe_table_name_is_rejected(cache: RunCache) -> None:
    """A table name that could escape ``base_dir`` raises."""
    with pytest.raises(ValueError):
        cache.lookup("../escape", pl.DataFrame({"k": ["a"]}), key="k")


def test_key_missing_an_input_does_not_see_its_changes(cache: RunCache) -> None:
    """A key that leaves out an input (here the text) hits on an edited row and returns its old value."""
    cache.store("thing", pl.DataFrame({"id": [1], "v": ["old"]}), key="id")
    edited = pl.DataFrame({"id": [1], "text": ["new"]})
    assert cache.lookup("thing", edited, key="id").miss_count == 0
    assert cache.fetch("thing", edited, key="id").collect()["v"].to_list() == ["old"]
