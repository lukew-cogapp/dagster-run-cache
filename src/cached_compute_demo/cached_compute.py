"""Recompute only the rows whose inputs changed since an asset's last materialisation.

The cache is the asset's own previous output, read back through its IO manager,
so it needs no storage beyond what the asset already writes and survives
container-per-run deployments.
"""

from collections.abc import Callable
from typing import cast

import dagster as dg
import polars as pl
import polars_hash as plh  # noqa: F401  (registers the .nchash Expr namespace)

HASH_COLUMN = "_cache_input_hash"


def input_hash(inputs: list[str], version: str) -> pl.Expr:
    """Stable per-row hash of the input columns plus a version salt.

    polars-hash's wyhash rather than ``pl.hash``, which is not stable across
    Polars versions: an upgrade would otherwise invalidate the whole cache.
    """
    row = pl.struct(*[pl.col(c) for c in inputs], pl.lit(version).alias("_version"))
    return row.struct.json_encode().nchash.wyhash().alias(HASH_COLUMN)  # type: ignore[attr-defined,no-any-return]


def _load_prior(context: dg.AssetExecutionContext) -> pl.LazyFrame | None:
    try:
        return cast(pl.LazyFrame, context.load_asset_value(context.asset_key, python_type=pl.LazyFrame))
    except FileNotFoundError:
        return None


def cached_compute(
    context: dg.AssetExecutionContext,
    current: pl.LazyFrame,
    *,
    key: list[str],
    inputs: list[str],
    compute: Callable[[pl.DataFrame], pl.DataFrame],
    version: str,
    batch_size: int = 500,
) -> dg.Output[pl.DataFrame]:
    """Return ``compute``'s result for every row of ``current``, reusing last run's where inputs are unchanged.

    Args:
        context: The calling asset's context; its own key is the cache.
        current: Rows to produce results for. Keys absent from it drop out of the cache.
        inputs: Columns whose change invalidates a row's cached result.
        compute: Takes a batch of ``key`` + ``inputs`` rows, returns ``key`` + result columns.
        version: Bump to invalidate every row, e.g. on a model or logic change.

    """
    current = current.select(*key, *inputs).with_columns(input_hash(inputs, version))
    match_on = [*key, HASH_COLUMN]
    prior = _load_prior(context)

    todo = current if prior is None else current.join(prior.select(match_on), on=match_on, how="anti")
    misses = todo.collect()

    parts: list[pl.DataFrame] = []
    if prior is not None:
        # Collected, not left lazy: prior scans the file the IO manager is about to overwrite.
        parts.append(prior.join(current.select(match_on), on=match_on, how="semi").collect())
    hits = parts[0].height if parts else 0
    for batch in misses.iter_slices(batch_size):
        result = compute(batch.drop(HASH_COLUMN))
        parts.append(result.join(batch.select(match_on), on=key, how="inner"))

    if parts:
        out = pl.concat(parts, how="diagonal_relaxed")
    else:
        out = current.select(match_on).collect()
    return dg.Output(
        out,
        metadata={
            "cache_hits": dg.MetadataValue.int(hits),
            "cache_misses": dg.MetadataValue.int(misses.height),
        },
    )
