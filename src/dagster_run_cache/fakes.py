"""Fake source data and fake slow services, standing in for TMS and real HTTP endpoints."""

import datetime as dt
import hashlib
import time

import polars as pl

EMBED_DIM = 8
EMBED_SECONDS_PER_ROW = 0.002
GEOCODE_SECONDS_PER_CALL = 0.05
IMAGE_SECONDS_PER_FILE = 0.003

ARTISTS = ["Hokusai", "Cassatt", "Turner", "Kahlo", "Hiroshige", "Morisot", "Sargent"]
MEDIA = ["woodblock print", "oil on canvas", "watercolour", "etching", "pastel"]
PLACES = [
    "Tokyo",
    "Paris",
    "London",
    "Mexico City",
    "Kyoto",
    "Boston",
    "Venice",
    "Osaka",
    "Florence",
    "New York",
    "Edo",
    "Madrid",
]
BASE_FILE_DATE = dt.date(2026, 1, 1)


def _row(i: int, place: str, date: dt.date) -> dict[str, object]:
    return {
        "doc_id": i,
        "title": f"Object {i}",
        "artist": ARTISTS[i % len(ARTISTS)],
        "medium": MEDIA[i % len(MEDIA)],
        "place": place,
        "file_name": f"img_{i:05}.tif",
        "file_date": date,
        "modified": date,
    }


def fake_documents(edition: int, size: int) -> pl.DataFrame:
    """Generate the source as it stands at ``edition``.

    Each edition after the first retitles 10 documents (a newer ``modified``),
    re-photographs 3 (a newer ``file_date``), deletes 5, and adds 5 sharing one new place.
    """
    rows = {i: _row(i, PLACES[i % len(PLACES)], BASE_FILE_DATE) for i in range(size)}
    for e in range(2, edition + 1):
        offset = (e - 2) * 10
        edited = BASE_FILE_DATE + dt.timedelta(days=e)
        for i in range(offset, offset + 10):
            rows[i]["title"] = f"Object {i} (revised in edition {e})"
            rows[i]["modified"] = edited
        for i in range(offset + 50, offset + 53):
            rows[i]["file_date"] = edited
        for i in range(size - offset - 5, size - offset):
            rows.pop(i, None)
        for i in range(size + offset, size + offset + 5):
            rows[i] = _row(i, f"New place {e}", edited)
    return pl.DataFrame(list(rows.values()))


def _digest(text: str) -> bytes:
    return hashlib.sha256(text.encode()).digest()


def embed_endpoint(texts: list[str], model: str) -> list[list[float]]:
    """Fake batched embedding API: one round trip per call, cost per text."""
    time.sleep(EMBED_SECONDS_PER_ROW * len(texts))
    return [[b / 255 for b in _digest(f"{model}:{t}")[:EMBED_DIM]] for t in texts]


def geocode_endpoint(place: str) -> tuple[float, float]:
    """Fake geocoder: one place per call, rate-limited like a public API."""
    time.sleep(GEOCODE_SECONDS_PER_CALL)
    d = _digest(place)
    return d[0] / 255 * 180 - 90, d[1] / 255 * 360 - 180


def analyse_image(file_name: str, file_date: dt.date) -> dict[str, object]:
    """Fake image decode: dimensions and a dominant colour, slow per file."""
    time.sleep(IMAGE_SECONDS_PER_FILE)
    d = _digest(f"{file_name}:{file_date}")
    return {"width": 1000 + d[0] * 10, "height": 1000 + d[1] * 10, "dominant_hex": f"#{d[2:5].hex()}"}
