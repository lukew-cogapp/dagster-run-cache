"""Demo assets: one fake source and four cached assets built from it with ``RunCache``."""

from pathlib import Path

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager

from dagster_run_cache import RunCache, fakes
from dagster_run_cache.utils.cached_io_manager import CachedParquetIOManager, uncached

EMBED_BATCH_SIZE = 100
VECTOR = pl.Array(pl.Float32, fakes.EMBED_DIM)


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


def _geocode(batch: pl.DataFrame) -> pl.DataFrame:
    coords = [fakes.geocode_endpoint(place) for place in batch["place"]]
    return batch.select("place").with_columns(
        lat=pl.Series([c[0] for c in coords]), lon=pl.Series([c[1] for c in coords])
    )


@dg.asset
def place_geocodes(context: dg.AssetExecutionContext, cache: RunCache, documents: pl.LazyFrame) -> pl.LazyFrame:
    """Coordinates per distinct place, each geocoded once ever."""
    result, lookup = cache.compute("geocode", documents.select("place").unique(), key="place", fn=_geocode)
    context.add_output_metadata(lookup.metadata)
    return result


def _embed(batch: pl.DataFrame) -> pl.DataFrame:
    vectors = fakes.embed_endpoint(batch["text"].to_list(), batch["model"][0] if batch.height else "")
    return batch.with_columns(pl.Series("vector", vectors, dtype=VECTOR))


def _embedding_text(documents: pl.LazyFrame, model: str) -> pl.LazyFrame:
    return documents.select(
        "doc_id",
        "modified",
        model=pl.lit(model),
        text=pl.concat_str("title", "artist", "medium", separator=". "),
    )


@dg.asset
def doc_embeddings(
    context: dg.AssetExecutionContext, config: EmbedConfig, cache: RunCache, documents: pl.LazyFrame
) -> pl.LazyFrame:
    """One vector per document from a batched embedding endpoint, keyed on the text itself.

    The key holds every input to the embedding, so ``fn`` needs nothing beyond the key and
    ``compute`` does the whole job. A new model misses everything.
    """
    docs = _embedding_text(documents, config.model)
    result, lookup = cache.compute("embed", docs, key=["model", "text"], fn=_embed, batch_size=EMBED_BATCH_SIZE)
    context.add_output_metadata(lookup.metadata)
    return result.select("doc_id", "vector").sort("doc_id")


@dg.asset
def doc_embeddings_by_id(
    context: dg.AssetExecutionContext, config: EmbedConfig, cache: RunCache, documents: pl.LazyFrame
) -> pl.LazyFrame:
    """Embed each document as ``doc_embeddings`` does, keyed on the record instead of its text.

    ``doc_id`` alone never changes, so an edited record would keep its old vector; ``modified``
    is what moves when the record is edited. The text is not in the key, but the endpoint needs
    it, so this uses ``lookup``, ``store`` and ``fetch``: the misses carry every column, and only
    the key and vector are stored.
    """
    docs = _embedding_text(documents, config.model)
    key = ["model", "doc_id", "modified"]

    lookup = cache.lookup("embed_by_id", docs, key=key)
    for batch in lookup.misses.iter_slices(EMBED_BATCH_SIZE):
        cache.store("embed_by_id", _embed(batch).select(*key, "vector"), key=key)

    context.add_output_metadata(lookup.metadata)
    return cache.fetch("embed_by_id", docs.select(key), key=key).select("doc_id", "vector").sort("doc_id")


@dg.asset(io_manager_key="cached_io_manager", metadata={"cache_key": ["model", "text"]})
def embedding_cache(context: dg.AssetExecutionContext, config: EmbedConfig, documents: pl.LazyFrame) -> pl.DataFrame:
    """Every vector this asset has embedded, kept by ``CachedParquetIOManager``.

    Returns only this run's new vectors; the IO manager merges them into the stored table.
    """
    lookup = uncached(context, _embedding_text(documents, config.model).select("model", "text"))
    context.add_output_metadata(lookup.metadata)
    keys = lookup.misses.unique(maintain_order=True)
    batches = [_embed(batch) for batch in keys.iter_slices(EMBED_BATCH_SIZE)]
    return pl.concat(batches) if batches else _embed(keys)


@dg.asset
def doc_embeddings_via_io(config: EmbedConfig, documents: pl.LazyFrame, embedding_cache: pl.LazyFrame) -> pl.LazyFrame:
    """One vector per current document, joined from ``embedding_cache``."""
    docs = _embedding_text(documents, config.model)
    return docs.join(embedding_cache, on=["model", "text"]).select("doc_id", "vector").sort("doc_id")


def _analyse(batch: pl.DataFrame) -> pl.DataFrame:
    results = [fakes.analyse_image(name, date) for name, date in batch.iter_rows()]
    return batch.with_columns(pl.DataFrame(results))


@dg.asset
def image_analysis(context: dg.AssetExecutionContext, cache: RunCache, documents: pl.LazyFrame) -> pl.LazyFrame:
    """Dimensions and dominant colour per image file, redone when the file date moves on."""
    files = documents.select("file_name", "file_date").unique()
    result, lookup = cache.compute("image", files, key=["file_name", "file_date"], fn=_analyse, batch_size=250)
    context.add_output_metadata(lookup.metadata)
    return result


ALL_ASSETS = [
    documents,
    place_geocodes,
    doc_embeddings,
    doc_embeddings_by_id,
    embedding_cache,
    doc_embeddings_via_io,
    image_analysis,
]
CACHE_TABLES = {
    "place_geocodes": "geocode",
    "doc_embeddings": "embed",
    "doc_embeddings_by_id": "embed_by_id",
    "embedding_cache": "embedding_cache",
    "image_analysis": "image",
}

defs = dg.Definitions(
    assets=ALL_ASSETS,
    resources={
        "io_manager": PolarsParquetIOManager(base_dir="output"),
        "cache": RunCache(base_dir="output/cache"),
        "cached_io_manager": CachedParquetIOManager(base_dir="output"),
    },
)


def run_demo(
    storage: Path, edition: int = 1, model: str = "fake-embed-v1", size: int = 1_000
) -> dict[str, tuple[int, int]]:
    """Materialise every asset once against ``storage``; returns (hits, misses) per cached asset."""
    result = dg.materialize(
        ALL_ASSETS,
        resources={
            "io_manager": PolarsParquetIOManager(base_dir=str(storage)),
            "cache": RunCache(base_dir=str(storage / "cache")),
            "cached_io_manager": CachedParquetIOManager(base_dir=str(storage)),
        },
        run_config={
            "ops": {
                "documents": {"config": {"edition": edition, "size": size}},
                "doc_embeddings": {"config": {"model": model}},
                "doc_embeddings_by_id": {"config": {"model": model}},
                "embedding_cache": {"config": {"model": model}},
                "doc_embeddings_via_io": {"config": {"model": model}},
            }
        },
    )
    if not result.success:
        raise RuntimeError("Demo run failed")
    counts = {}
    for asset, table in CACHE_TABLES.items():
        meta = result.asset_materializations_for_node(asset)[0].metadata
        counts[asset] = (meta[f"cache/{table}/hits"].value, meta[f"cache/{table}/misses"].value)
    return counts  # type: ignore[return-value]
