"""Demo assets: one fake source and three expensive steps cached against it with ``RunCache``."""

from pathlib import Path

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager

from dagster_run_cache import RunCache, fakes

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


@dg.asset
def doc_embeddings(
    context: dg.AssetExecutionContext, config: EmbedConfig, cache: RunCache, documents: pl.LazyFrame
) -> pl.LazyFrame:
    """One vector per document from a batched embedding endpoint.

    Uses ``lookup``, ``store`` and ``fetch`` directly rather than ``compute``, to show the steps
    ``compute`` runs. The model is part of the key, so a new model misses everything.
    """
    docs = documents.select(
        "doc_id",
        model=pl.lit(config.model),
        text=pl.concat_str("title", "artist", "medium", separator=". "),
    )
    key = ["model", "text"]

    lookup = cache.lookup("embed", docs, key=key)
    for batch in lookup.misses.select(key).unique(maintain_order=True).iter_slices(EMBED_BATCH_SIZE):
        vectors = fakes.embed_endpoint(batch["text"].to_list(), config.model)
        cache.store("embed", batch.with_columns(pl.Series("vector", vectors, dtype=VECTOR)), key=key)

    context.add_output_metadata(lookup.metadata)
    return cache.fetch("embed", docs, key=key).select("doc_id", "vector").sort("doc_id")


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


ALL_ASSETS = [documents, place_geocodes, doc_embeddings, image_analysis]
CACHE_TABLES = {"place_geocodes": "geocode", "doc_embeddings": "embed", "image_analysis": "image"}

defs = dg.Definitions(
    assets=ALL_ASSETS,
    resources={
        "io_manager": PolarsParquetIOManager(base_dir="output"),
        "cache": RunCache(base_dir="output/cache"),
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
        },
        run_config={
            "ops": {
                "documents": {"config": {"edition": edition, "size": size}},
                "doc_embeddings": {"config": {"model": model}},
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
