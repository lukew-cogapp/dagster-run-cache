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


def test_missing_on_empty_cache_returns_every_row(cache: RunCache) -> None:
    """With nothing stored yet, every row is a miss."""
    frame = pl.DataFrame({"k": ["a", "b"]})
    assert cache.missing("thing", frame, key="k").equals(frame)


def test_store_then_missing_returns_only_new_rows(cache: RunCache) -> None:
    """Stored keys stop being misses; unstored ones remain."""
    cache.store("thing", _rows(("a", 1), ("b", 2)), key="k")
    misses = cache.missing("thing", pl.DataFrame({"k": ["a", "b", "c"]}), key="k")
    assert misses["k"].to_list() == ["c"]


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


def test_multi_column_key(cache: RunCache) -> None:
    """A key of several columns matches only when every column matches."""
    cache.store("thing", pl.DataFrame({"name": ["a"], "date": [1], "v": [10]}), key=["name", "date"])
    frame = pl.DataFrame({"name": ["a", "a"], "date": [1, 2]})
    assert cache.missing("thing", frame, key=["name", "date"])["date"].to_list() == [2]


def test_store_leaves_one_file_and_no_temp(cache: RunCache, tmp_path: Path) -> None:
    """Repeated stores keep a single file per prefix with no temp files beside it."""
    cache.store("thing", _rows(("a", 1)), key="k")
    cache.store("thing", _rows(("b", 2)), key="k")
    assert [p.name for p in tmp_path.iterdir()] == ["thing.parquet"]


def test_store_rejects_changed_columns(cache: RunCache) -> None:
    """Storing a different schema raises rather than mixing shapes in one file."""
    cache.store("thing", _rows(("a", 1)), key="k")
    with pytest.raises(ValueError, match="clear"):
        cache.store("thing", pl.DataFrame({"k": ["b"], "other": [1.0]}), key="k")


def test_clear_counts_and_removes_one_prefix(cache: RunCache) -> None:
    """``clear`` removes one prefix's rows and leaves other prefixes alone."""
    cache.store("thing", _rows(("a", 1), ("b", 2)), key="k")
    cache.store("other", _rows(("a", 1)), key="k")
    assert cache.clear("thing") == 2
    assert cache.clear("thing") == 0
    assert cache.missing("other", pl.DataFrame({"k": ["a"]}), key="k").is_empty()


def test_compute_calls_fn_once_per_uncached_key(cache: RunCache) -> None:
    """``compute`` sends only uncached keys to ``fn``, once each, and returns every row with its value."""
    seen: list[list[str]] = []

    def fn(batch: pl.DataFrame) -> pl.DataFrame:
        seen.append(batch["k"].to_list())
        return batch.select("k", v=pl.col("k").str.len_chars())

    first = cache.compute("thing", pl.DataFrame({"k": ["a", "bb", "a"]}), key="k", fn=fn)
    second = cache.compute("thing", pl.DataFrame({"k": ["a", "ccc"]}), key="k", fn=fn)

    assert seen == [["a", "bb"], ["ccc"]]
    assert first.sort("k")["v"].to_list() == [1, 1, 2]
    assert second.sort("k")["v"].to_list() == [1, 3]


def test_unsafe_prefix_is_rejected(cache: RunCache) -> None:
    """A prefix that could escape ``base_dir`` raises."""
    with pytest.raises(ValueError):
        cache.missing("../escape", pl.DataFrame({"k": ["a"]}), key="k")


def test_take_stats_counts_and_resets(cache: RunCache) -> None:
    """Rows found count as hits, the rest as misses, and taking the stats resets them."""
    cache.store("thing", _rows(("a", 1)), key="k")
    cache.missing("thing", pl.DataFrame({"k": ["a", "b", "c"]}), key="k")
    assert cache.take_stats() == {"cache_hits": 1, "cache_misses": 2}
    assert cache.take_stats() == {"cache_hits": 0, "cache_misses": 0}
