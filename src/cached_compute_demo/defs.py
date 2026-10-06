"""Demo assets: a fake document source and an embedding asset cached with ``cached_compute``."""

import hashlib
import time

import dagster as dg
import polars as pl

from cached_compute_demo.cached_compute import cached_compute

EMBED_DIM = 8
EMBED_SECONDS_PER_ROW = 0.002


class SourceConfig(dg.Config):
    """Which edit of the fake source to serve, standing in for TMS changing between nightly runs."""

    edition: int = 1
    size: int = 1_000


def fake_documents(edition: int, size: int) -> pl.DataFrame:
    """Generate the source as it stands at ``edition``.

    Each edition after the first retitles 10 documents, deletes 5 and adds 5.
    """
    ids = list(range(size))
    titles = {i: f"Object {i}" for i in ids}
    for e in range(2, edition + 1):
        offset = (e - 2) * 10
        for i in range(offset, offset + 10):
            titles[i] = f"Object {i} (revised in edition {e})"
        for i in range(size - offset - 5, size - offset):
            titles.pop(i, None)
        for i in range(size + offset, size + offset + 5):
            titles[i] = f"Object {i}"
    return pl.DataFrame({"doc_id": list(titles), "title": list(titles.values())})


def fake_embed(texts: pl.Series) -> pl.Series:
    """Deterministic stand-in for a model call, slow enough per row that a cache hit is visible."""
    time.sleep(EMBED_SECONDS_PER_ROW * len(texts))
    vectors = [[b / 255 for b in hashlib.sha256(t.encode()).digest()[:EMBED_DIM]] for t in texts]
    return pl.Series("vector", vectors, dtype=pl.Array(pl.Float32, EMBED_DIM))


class EmbedConfig(dg.Config):
    """Embedding model version; changing it invalidates every cached vector."""

    version: str = "fake-embed-v1"


@dg.asset
def documents(config: SourceConfig) -> pl.DataFrame:
    """Serve the fake upstream source at the configured edition."""
    return fake_documents(config.edition, config.size)


@dg.asset
def doc_embeddings(
    context: dg.AssetExecutionContext, config: EmbedConfig, documents: pl.LazyFrame
) -> dg.Output[pl.DataFrame]:
    """One vector per document, embedding only documents whose title changed since the last run."""
    return cached_compute(
        context,
        documents,
        key=["doc_id"],
        inputs=["title"],
        compute=lambda batch: batch.select("doc_id", fake_embed(batch["title"])),
        version=config.version,
    )
