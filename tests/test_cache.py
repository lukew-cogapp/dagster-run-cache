from datetime import timedelta
from pathlib import Path

import polars as pl
import pytest

from dagster_run_cache import content_key
from dagster_run_cache.utils import cache as cache_module
from dagster_run_cache.utils.cache import RunCache


@pytest.fixture
def cache(tmp_path: Path) -> RunCache:
    """Return a cache in a fresh directory."""
    return RunCache(base_dir=str(tmp_path))


def test_get_returns_default_on_miss(cache: RunCache) -> None:
    """An absent key returns the default."""
    assert cache.get("thing:a") is None
    assert cache.get("thing:a", "fallback") == "fallback"


def test_set_then_get_round_trips_any_picklable_value(cache: RunCache) -> None:
    """Values come back equal, including types JSON cannot hold."""
    value = {"coords": (37.7, -122.4), "tags": {"a", "b"}, "vector": [0.1, 0.2]}
    cache.set("thing:a", value)
    assert cache.get("thing:a") == value


def test_entries_persist_across_instances(tmp_path: Path) -> None:
    """A second resource on the same directory, as in the next run, sees the entry."""
    RunCache(base_dir=str(tmp_path)).set("thing:a", 1)
    assert RunCache(base_dir=str(tmp_path)).get("thing:a") == 1


def test_has_and_delete(cache: RunCache) -> None:
    """``has`` reflects presence and ``delete`` reports whether the key existed."""
    cache.set("thing:a", 1)
    assert cache.has("thing:a")
    assert cache.delete("thing:a")
    assert not cache.has("thing:a")
    assert not cache.delete("thing:a")


def test_get_or_set_calls_factory_once(cache: RunCache) -> None:
    """The factory runs on the first call only."""
    calls = []

    def factory() -> int:
        calls.append(1)
        return 42

    assert cache.get_or_set("thing:a", factory) == 42
    assert cache.get_or_set("thing:a", factory) == 42
    assert len(calls) == 1


def test_get_many_returns_hits_only(cache: RunCache) -> None:
    """Misses are left out of the result rather than mapped to None."""
    cache.set_many({"thing:a": 1, "thing:b": 2})
    assert cache.get_many(["thing:a", "thing:b", "thing:c"]) == {"thing:a": 1, "thing:b": 2}


def test_add_stores_only_when_absent(cache: RunCache) -> None:
    """``add`` refuses to overwrite an existing key."""
    assert cache.add("thing:a", 1)
    assert not cache.add("thing:a", 2)
    assert cache.get("thing:a") == 1


def test_clear_removes_one_prefix(cache: RunCache) -> None:
    """Clearing a prefix leaves other prefixes alone."""
    cache.set_many({"embed:a": 1, "embed:b": 2, "geocode:a": 3})
    assert cache.clear("embed") == 2
    assert cache.get_many(["embed:a", "embed:b", "geocode:a"]) == {"geocode:a": 3}


def test_ttl_expires_entries(cache: RunCache, monkeypatch: pytest.MonkeyPatch) -> None:
    """An entry past its TTL reads as absent."""
    monkeypatch.setattr(cache_module, "_now", lambda: 1_000.0)
    cache.set("thing:a", 1, ttl=timedelta(seconds=10))
    assert cache.get("thing:a") == 1

    monkeypatch.setattr(cache_module, "_now", lambda: 1_011.0)
    assert not cache.has("thing:a")
    assert cache.get("thing:a") is None


def test_corrupt_entry_reads_as_miss(cache: RunCache) -> None:
    """A damaged file is treated as absent rather than failing the run."""
    cache.set("thing:a", 1)
    cache._path("thing:a").write_bytes(b"not a pickle")
    assert cache.get("thing:a") is None
    assert not cache.has("thing:a")


def test_set_leaves_no_temp_files(cache: RunCache, tmp_path: Path) -> None:
    """Only the entry itself remains after a write."""
    cache.set("thing:a", 1)
    assert [p.suffix for p in tmp_path.rglob("*") if p.is_file()] == [".pkl"]


def test_unsafe_prefix_is_rejected(cache: RunCache) -> None:
    """A prefix that could escape ``base_dir`` raises."""
    with pytest.raises(ValueError):
        cache.set("../escape:a", 1)


def test_take_stats_counts_and_resets(cache: RunCache) -> None:
    """Hits and misses accumulate across reads and reset when taken."""
    cache.set("thing:a", 1)
    cache.get("thing:a")
    cache.get("thing:b")
    cache.get_many(["thing:a", "thing:c"])
    assert cache.take_stats() == {"cache_hits": 2, "cache_misses": 2}
    assert cache.take_stats() == {"cache_hits": 0, "cache_misses": 0}


def test_content_key_changes_with_any_part() -> None:
    """Same parts give the same key; changing one part gives a new key."""
    assert content_key("embed", "v1", "text") == content_key("embed", "v1", "text")
    assert content_key("embed", "v1", "text") != content_key("embed", "v2", "text")
    assert content_key("embed", "v1", "text").startswith("embed:")


def _rows(*pairs: tuple[str, int]) -> pl.DataFrame:
    return pl.DataFrame({"k": [p[0] for p in pairs], "v": [p[1] for p in pairs]})


def test_missing_on_empty_table_returns_every_row(cache: RunCache) -> None:
    """With no table stored yet, every row is a miss."""
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


def test_store_replaces_existing_keys_and_keeps_the_rest(cache: RunCache, tmp_path: Path) -> None:
    """A second store overwrites shared keys, adds new ones and leaves the others."""
    cache.store("thing", _rows(("a", 1), ("b", 2)), key="k")
    cache.store("thing", _rows(("b", 20), ("c", 3)), key="k")
    table = pl.read_parquet(tmp_path / "thing" / "table.parquet").sort("k")
    assert table.to_dicts() == [{"k": "a", "v": 1}, {"k": "b", "v": 20}, {"k": "c", "v": 3}]


def test_store_keeps_last_of_duplicate_keys(cache: RunCache) -> None:
    """Duplicate keys within one store collapse to the last row."""
    cache.store("thing", _rows(("a", 1), ("a", 2)), key="k")
    assert cache.fetch("thing", pl.DataFrame({"k": ["a"]}), key="k").collect()["v"].to_list() == [2]


def test_store_leaves_one_file_and_no_temp(cache: RunCache, tmp_path: Path) -> None:
    """Repeated stores keep a single table file with no temp files beside it."""
    cache.store("thing", _rows(("a", 1)), key="k")
    cache.store("thing", _rows(("b", 2)), key="k")
    assert [p.name for p in tmp_path.rglob("*") if p.is_file()] == ["table.parquet"]


def test_store_rejects_changed_columns(cache: RunCache) -> None:
    """Storing a different schema raises rather than mixing shapes in one table."""
    cache.store("thing", _rows(("a", 1)), key="k")
    with pytest.raises(ValueError, match="clear"):
        cache.store("thing", pl.DataFrame({"k": ["b"], "other": [1.0]}), key="k")


def test_clear_counts_and_removes_the_table(cache: RunCache) -> None:
    """``clear`` removes table rows and per-key entries under the prefix together."""
    cache.store("thing", _rows(("a", 1), ("b", 2)), key="k")
    cache.set("thing:x", 1)
    assert cache.clear("thing") == 3
    assert cache.missing("thing", pl.DataFrame({"k": ["a"]}), key="k").height == 1


def test_missing_counts_towards_stats(cache: RunCache) -> None:
    """Rows found in the table count as hits, the rest as misses."""
    cache.store("thing", _rows(("a", 1)), key="k")
    cache.missing("thing", pl.DataFrame({"k": ["a", "b", "c"]}), key="k")
    assert cache.take_stats() == {"cache_hits": 1, "cache_misses": 2}
