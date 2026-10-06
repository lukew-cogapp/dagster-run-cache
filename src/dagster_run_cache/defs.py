"""Demo assets: one fake source and three ways of using ``RunCache`` against it."""

import functools
import itertools
from datetime import timedelta

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager

from dagster_run_cache import RunCache, content_key, fakes

EMBED_BATCH_SIZE = 100
# Re-analyse each image monthly even if its file date never changes.
IMAGE_TTL = timedelta(days=30)


class SourceConfig(dg.Config):
    """Which edit of the fake source to serve, standing in for TMS changing between nightly runs."""

    edition: int = 1
    size: int = 1_000


class EmbedConfig(dg.Config):
    """Embedding model name; it is part of every embedding key."""

    model: str = "fake-embed-v1"


@dg.asset
def documents(config: SourceConfig) -> pl.DataFrame:
    """Fake upstream source documents."""
    return fakes.fake_documents(config.edition, config.size)


@dg.asset
def place_geocodes(context: dg.AssetExecutionContext, cache: RunCache, documents: pl.LazyFrame) -> pl.DataFrame:
    """Coordinates per distinct place, each geocoded once ever.

    Pattern: ``get_or_set`` with a readable natural key.
    """
    places = documents.select("place").unique().sort("place").collect()["place"].to_list()
    coords = [
        cache.get_or_set(f"geocode:{place}", functools.partial(fakes.geocode_endpoint, place)) for place in places
    ]

    context.add_output_metadata(cache.take_stats())
    return pl.DataFrame({"place": places, "lat": [c[0] for c in coords], "lon": [c[1] for c in coords]})


@dg.asset
def doc_embeddings(
    context: dg.AssetExecutionContext, config: EmbedConfig, cache: RunCache, documents: pl.LazyFrame
) -> pl.DataFrame:
    """One vector per document from a batched embedding endpoint.

    Pattern: ``get_many``, send only the misses to the endpoint in batches, ``set_many`` the results.
    The key hashes the model and the text, so an edit or a new model is a new key.
    """
    docs = documents.select("doc_id", text=pl.concat_str("title", "artist", "medium", separator=". ")).collect()
    keys = [content_key("embed", config.model, text) for text in docs["text"]]

    vectors = cache.get_many(keys)
    missing = {key: text for key, text in zip(keys, docs["text"], strict=True) if key not in vectors}
    for batch in itertools.batched(missing.items(), EMBED_BATCH_SIZE, strict=False):
        batch_keys, texts = zip(*batch, strict=True)
        fresh = dict(zip(batch_keys, fakes.embed_endpoint(list(texts), config.model), strict=True))
        cache.set_many(fresh)
        vectors |= fresh

    context.add_output_metadata(cache.take_stats())
    return docs.select(
        "doc_id",
        pl.Series("vector", [vectors[key] for key in keys], dtype=pl.Array(pl.Float32, fakes.EMBED_DIM)),
    )


@dg.asset
def image_analysis(context: dg.AssetExecutionContext, cache: RunCache, documents: pl.LazyFrame) -> pl.DataFrame:
    """Dimensions and dominant colour per image file.

    Pattern: plain cache-aside with ``get`` and ``set``, plus a TTL. The file date is part of
    the key, so a re-photographed image is a new key.
    """
    files = documents.select("file_name", "file_date").unique().sort("file_name").collect()

    rows = []
    for file_name, file_date in files.iter_rows():
        key = content_key("image", file_name, file_date)
        result = cache.get(key)
        if result is None:
            result = fakes.analyse_image(file_name, file_date)
            cache.set(key, result, ttl=IMAGE_TTL)
        rows.append({"file_name": file_name, **result})

    context.add_output_metadata(cache.take_stats())
    return pl.DataFrame(rows)


ALL_ASSETS = [documents, place_geocodes, doc_embeddings, image_analysis]

defs = dg.Definitions(
    assets=ALL_ASSETS,
    resources={
        "io_manager": PolarsParquetIOManager(base_dir="output"),
        "cache": RunCache(base_dir="output/cache"),
    },
)
