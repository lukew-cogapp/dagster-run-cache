"""Demo: a fake source, two fake services that cache through ``Cache``, and the assets using them."""

from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager

from dagster_run_cache import Cache, Lookup, ParquetCache, fakes
from dagster_run_cache.utils.cache import cache_metadata

EMBED_BATCH_SIZE = 100
VECTOR = pl.Array(pl.Float32, fakes.EMBED_DIM)


class SourceConfig(dg.Config):
    """Which edit of the fake source to serve, standing in for TMS changing between nightly runs."""

    edition: int = 1
    size: int = 1_000


class Geocoder(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Fake geocoding service that checks its cache before every call."""

    cache: Cache

    def coords(self, place: str) -> list[float]:
        """Return ``[lat, lon]`` for ``place``, calling the service only on a cache miss."""
        cached: list[float] | None = self.cache.get(place)
        if cached is not None:
            return cached
        result = list(fakes.geocode_endpoint(place))
        self.cache.set(place, result)
        return result


class Embedder(dg.ConfigurableResource):  # type: ignore[type-arg]
    """Fake embedding service that only sends its cache's misses to the endpoint."""

    model: str = "fake-embed-v1"
    cache: Cache

    def embed(self, frame: pl.LazyFrame) -> tuple[pl.LazyFrame, Lookup]:
        """Join a ``vector`` onto each row of ``frame`` by its ``text``, embedding only uncached texts."""
        texts = frame.with_columns(model=pl.lit(self.model))
        return self.cache.compute(texts, key=["model", "text"], fn=self.vectors, batch_size=EMBED_BATCH_SIZE)

    def vectors(self, batch: pl.DataFrame) -> pl.DataFrame:
        """Call the endpoint for every row of ``batch``, with no cache."""
        vectors = fakes.embed_endpoint(batch["text"].to_list(), self.model)
        return batch.with_columns(pl.Series("vector", vectors, VECTOR))


def _texts(documents: pl.LazyFrame) -> pl.LazyFrame:
    return documents.select("doc_id", "modified", text=pl.concat_str("title", "artist", "medium", separator=". "))


@dg.asset
def documents(config: SourceConfig) -> pl.DataFrame:
    """Fake upstream source documents."""
    return fakes.fake_documents(config.edition, config.size)


@dg.asset
def place_geocodes(context: dg.AssetExecutionContext, geocoder: Geocoder, documents: pl.LazyFrame) -> pl.DataFrame:
    """Coordinates per distinct place, through a geocoder that caches per key."""
    places = documents.select("place").unique().sort("place").collect()["place"].to_list()
    hits = sum(geocoder.cache.has(place) for place in places)
    coords = [geocoder.coords(place) for place in places]
    context.add_output_metadata(cache_metadata(geocoder.cache.name, hits, len(places) - hits))
    return pl.DataFrame({"place": places, "lat": [c[0] for c in coords], "lon": [c[1] for c in coords]})


@dg.asset
def doc_embeddings(context: dg.AssetExecutionContext, embedder: Embedder, documents: pl.LazyFrame) -> pl.LazyFrame:
    """One vector per document; the embedder resource does the caching."""
    result, lookup = embedder.embed(_texts(documents))
    context.add_output_metadata(lookup.metadata)
    return result.select("doc_id", "vector").sort("doc_id")


@dg.asset
def doc_embeddings_by_id(
    context: dg.AssetExecutionContext, embedder: Embedder, embed_by_id_cache: Cache, documents: pl.LazyFrame
) -> pl.LazyFrame:
    """Embed each document as ``doc_embeddings`` does, cached by record rather than by text.

    ``doc_id`` alone never changes, so an edited record would keep its old vector; ``modified``
    moves when the record is edited. The endpoint needs the text, which is not in the key, so
    this uses ``lookup``, ``store`` and ``fetch``: the misses carry every column.
    """
    docs = _texts(documents).with_columns(model=pl.lit(embedder.model))
    key = ["model", "doc_id", "modified"]

    lookup = embed_by_id_cache.lookup(docs, key)
    for batch in lookup.misses.iter_slices(EMBED_BATCH_SIZE):
        embed_by_id_cache.store(embedder.vectors(batch).select(*key, "vector"), key)

    context.add_output_metadata(lookup.metadata)
    return embed_by_id_cache.fetch(docs.select(key), key).select("doc_id", "vector").sort("doc_id")


def _analyse(batch: pl.DataFrame) -> pl.DataFrame:
    results = [fakes.analyse_image(name, date) for name, date in batch.iter_rows()]
    return batch.with_columns(pl.DataFrame(results))


@dg.asset
def image_analysis(context: dg.AssetExecutionContext, image_cache: Cache, documents: pl.LazyFrame) -> pl.LazyFrame:
    """Dimensions and dominant colour per image file, with ``compute`` called from the asset."""
    files = documents.select("file_name", "file_date").unique()
    result, lookup = image_cache.compute(files, key=["file_name", "file_date"], fn=_analyse, batch_size=250)
    context.add_output_metadata(lookup.metadata)
    return result


ALL_ASSETS = [
    documents,
    place_geocodes,
    doc_embeddings,
    doc_embeddings_by_id,
    image_analysis,
]
CACHE_NAMES = {
    "place_geocodes": "geocode",
    "doc_embeddings": "embed",
    "doc_embeddings_by_id": "embed_by_id",
    "image_analysis": "image",
}


def resources(storage: Path, model: str = "fake-embed-v1") -> dict[str, Any]:
    """Wire every resource under ``storage``: asset outputs at its root, caches in ``storage/cache``."""
    cache_dir = str(storage / "cache")
    return {
        "io_manager": PolarsParquetIOManager(base_dir=str(storage)),
        "geocoder": Geocoder(cache=ParquetCache(name="geocode", base_dir=cache_dir)),
        "embedder": Embedder(model=model, cache=ParquetCache(name="embed", base_dir=cache_dir)),
        "embed_by_id_cache": ParquetCache(name="embed_by_id", base_dir=cache_dir),
        "image_cache": ParquetCache(name="image", base_dir=cache_dir),
    }


defs = dg.Definitions(assets=ALL_ASSETS, resources=resources(Path("output")))


def run_demo(
    storage: Path, edition: int = 1, model: str = "fake-embed-v1", size: int = 1_000
) -> dict[str, tuple[int, int]]:
    """Materialise every asset once against ``storage``; returns (hits, misses) per cached asset."""
    result = dg.materialize(
        ALL_ASSETS,
        resources=resources(storage, model),
        run_config={"ops": {"documents": {"config": {"edition": edition, "size": size}}}},
    )
    if not result.success:
        raise RuntimeError("Demo run failed")
    counts = {}
    for asset, name in CACHE_NAMES.items():
        meta = result.asset_materializations_for_node(asset)[0].metadata
        counts[asset] = (meta[f"cache/{name}/hits"].value, meta[f"cache/{name}/misses"].value)
    return counts  # type: ignore[return-value]
