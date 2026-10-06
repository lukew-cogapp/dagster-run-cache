"""A Redis-style key-value cache that persists between Dagster runs.

Each entry is its own file under ``base_dir``, with no shared index or database
to lock, so the cache is safe on a network mount shared by one container per run.
"""

import hashlib
import os
import pickle
import re
import shutil
import tempfile
import time
import zlib
from collections.abc import Callable, Iterable, Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import dagster as dg
from pydantic import PrivateAttr

_MISSING = object()
_PREFIX = re.compile(r"[\w.-]+")


def _now() -> float:
    return time.time()


def content_key(prefix: str, *parts: object) -> str:
    """Build ``prefix:<sha256 of parts>``, so changing any part gives a new key."""
    return f"{prefix}:{hashlib.sha256(repr(parts).encode()).hexdigest()}"


class RunCache(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Key-value cache shared by every run pointing at the same ``base_dir``.

    Keys follow the Redis ``prefix:rest`` convention, and each prefix is a
    directory that ``clear`` removes as a unit. Values are pickled, so only the
    pipeline should be able to write to ``base_dir``.
    """

    base_dir: str = "output/cache"

    _hits: int = PrivateAttr(default=0)
    _misses: int = PrivateAttr(default=0)

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value for ``key``, or ``default`` if absent or expired."""
        value = self._read(key)
        if value is _MISSING:
            self._misses += 1
            return default
        self._hits += 1
        return value

    def get_many(self, keys: Iterable[str]) -> dict[str, Any]:
        """Return the keys that are present, like ``MGET`` with the misses left out."""
        found = {}
        for key in keys:
            value = self.get(key, _MISSING)
            if value is not _MISSING:
                found[key] = value
        return found

    def get_or_set[T](self, key: str, factory: Callable[[], T], ttl: timedelta | None = None) -> T:
        """Return the cached value, or call ``factory``, store its result and return that."""
        value = self.get(key, _MISSING)
        if value is _MISSING:
            value = factory()
            self.set(key, value, ttl)
        return cast(T, value)

    def has(self, key: str) -> bool:
        """Check presence without loading the value."""
        try:
            with self._path(key).open("rb") as f:
                expires_at = pickle.load(f)
        except Exception:
            return False
        return expires_at is None or expires_at > _now()

    def set(self, key: str, value: Any, ttl: timedelta | None = None) -> None:
        """Store ``value`` under ``key``, replacing any existing entry."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        expires_at = _now() + ttl.total_seconds() if ttl else None
        # Written beside the target and renamed over it, so a reader never sees half an entry.
        fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        with os.fdopen(fd, "wb") as f:
            pickle.dump(expires_at, f)
            f.write(zlib.compress(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)))
        os.replace(tmp, path)

    def set_many(self, items: Mapping[str, Any], ttl: timedelta | None = None) -> None:
        """Store every key/value pair in ``items``."""
        for key, value in items.items():
            self.set(key, value, ttl)

    def add(self, key: str, value: Any, ttl: timedelta | None = None) -> bool:
        """Store ``value`` only if ``key`` is absent; returns whether it was stored.

        Not atomic: two runs adding the same key at once may both succeed.
        """
        if self.has(key):
            return False
        self.set(key, value, ttl)
        return True

    def delete(self, key: str) -> bool:
        """Remove ``key``; returns whether it existed."""
        try:
            self._path(key).unlink()
        except FileNotFoundError:
            return False
        return True

    def clear(self, prefix: str) -> int:
        """Remove every key under ``prefix``; returns how many there were."""
        directory = Path(self.base_dir) / self._check_prefix(prefix)
        count = sum(1 for _ in directory.rglob("*.pkl"))
        shutil.rmtree(directory, ignore_errors=True)
        return count

    def take_stats(self) -> dict[str, int]:
        """Return hit and miss counts since the last call, then reset them."""
        stats = {"cache_hits": self._hits, "cache_misses": self._misses}
        self._hits = self._misses = 0
        return stats

    def _check_prefix(self, prefix: str) -> str:
        if not _PREFIX.fullmatch(prefix):
            raise ValueError(f"Cache key prefix {prefix!r} must be letters, digits, '_', '.' or '-'")
        return prefix

    def _path(self, key: str) -> Path:
        prefix = self._check_prefix(key.split(":", 1)[0]) if ":" in key else "_"
        digest = hashlib.sha256(key.encode()).hexdigest()
        return Path(self.base_dir) / prefix / digest[:2] / f"{digest}.pkl"

    def _read(self, key: str) -> Any:
        path = self._path(key)
        try:
            with path.open("rb") as f:
                expires_at = pickle.load(f)
                if expires_at is not None and expires_at <= _now():
                    path.unlink(missing_ok=True)
                    return _MISSING
                return pickle.loads(zlib.decompress(f.read()))
        except FileNotFoundError:
            return _MISSING
        except Exception:
            # Truncated, corrupt, or pickled from a class that has since moved: recompute rather than fail the run.
            return _MISSING
