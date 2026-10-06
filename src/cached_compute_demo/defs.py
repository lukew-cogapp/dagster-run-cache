"""Demo assets: one fake source and three ways of caching expensive work against it."""

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager

from cached_compute_demo import fakes
from cached_compute_demo.cached_compute import cached_compute


class SourceConfig(dg.Config):
    """Which edit of the fake source to serve, standing in for TMS changing between nightly runs."""

    edition: int = 1
    size: int = 1_000


class EmbedConfig(dg.Config):
    """Embedding model version; changing it invalidates every cached vector."""

    model: str = "fake-embed-v1"


@dg.asset
def documents(config: SourceConfig) -> pl.DataFrame:
    """Fake upstream source documents."""
    return fakes.fake_documents(config.edition, config.size)


def _embed_batch(batch: pl.DataFrame, model: str) -> pl.DataFrame:
    texts = batch.select(pl.concat_str("title", "artist", "medium", separator=". "))
    vectors = fakes.embed_endpoint(texts.to_series().to_list(), model)
    return batch.select("doc_id", pl.Series("vector", vectors, dtype=pl.Array(pl.Float32, fakes.EMBED_DIM)))


@dg.asset
def doc_embeddings(
    context: dg.AssetExecutionContext, config: EmbedConfig, documents: pl.LazyFrame
) -> dg.Output[pl.DataFrame]:
    """One vector per document from a batched embedding endpoint."""
    return cached_compute(
        context,
        documents,
        key=["doc_id"],
        inputs=["title", "artist", "medium"],
        compute=lambda batch: _embed_batch(batch, config.model),
        version=config.model,
        batch_size=100,
    )


def _geocode_batch(batch: pl.DataFrame) -> pl.DataFrame:
    coords = [fakes.geocode_endpoint(p) for p in batch["place"]]
    return batch.select("place").with_columns(
        lat=pl.Series([c[0] for c in coords]),
        lon=pl.Series([c[1] for c in coords]),
    )


@dg.asset
def place_geocodes(context: dg.AssetExecutionContext, documents: pl.LazyFrame) -> dg.Output[pl.DataFrame]:
    """Coordinates per distinct place, so a place shared by many documents is looked up once."""
    return cached_compute(
        context,
        documents.select("place").unique(),
        key=["place"],
        inputs=[],
        compute=_geocode_batch,
        version="fake-geocoder-v1",
    )


def _analyse_batch(batch: pl.DataFrame) -> pl.DataFrame:
    results = [fakes.analyse_image(f, d) for f, d in batch.select("file_name", "file_date").iter_rows()]
    return batch.select("file_name").with_columns(pl.DataFrame(results))


@dg.asset
def image_analysis(context: dg.AssetExecutionContext, documents: pl.LazyFrame) -> dg.Output[pl.DataFrame]:
    """Dimensions and dominant colour per image file, redone only when the file is replaced."""
    return cached_compute(
        context,
        documents.select("file_name", "file_date").unique(),
        key=["file_name"],
        inputs=["file_date"],
        compute=_analyse_batch,
        version="fake-analyser-v1",
    )


ALL_ASSETS = [documents, doc_embeddings, place_geocodes, image_analysis]

defs = dg.Definitions(
    assets=ALL_ASSETS,
    resources={"io_manager": PolarsParquetIOManager(base_dir="output")},
)
