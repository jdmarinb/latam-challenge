import time
import polars as pl
from src.common.utils import read_polars as extractor
from src.common.logger import canonical_logger

# Modular Functional Blocks (KISS + Type Hints + Docstrings)


def mention_extractor(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Extracts all mentioned usernames from tweets."""
    return lf.select(
        pl.col("mentionedUsers")
        .explode()
        .struct.field("username")
        .str.to_lowercase()
        .alias("username")
    ).filter(pl.col("username").is_not_null() & (pl.col("username") != ""))


def mention_counter(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Counts occurrences of each mentioned username."""
    return lf.group_by("username").len()


def get_top_k(lf: pl.LazyFrame, k: int) -> pl.LazyFrame:
    """Selects the top k mentions based on frequency."""
    return lf.top_k(k, by="len")


@canonical_logger(event_name="q3_time_execution")
def q3_time(file_path: str, ctx=None) -> list[tuple[str, int]]:
    """
    Computes the top 10 mentioned users using a clean LazyFrame query.
    Uses Native Typing (Polars Schema) for ultra-fast validation and Canonical Logging.
    """
    if ctx:
        ctx.add_context(file_path=file_path)

    # Orchestrated pipeline
    t0 = time.perf_counter()
    query = (
        extractor(file_path)
        .pipe(mention_extractor)
        .pipe(mention_counter)
        .pipe(get_top_k, k=10)
        .sort(["len", "username"], descending=[True, False])
    )
    if ctx:
        ctx.add_step("build_query_plan", round((time.perf_counter() - t0) * 1000, 4))

    # SINGLE EXECUTION (Validation happens here at Rust level via schema)
    t0 = time.perf_counter()
    result = query.collect()

    if ctx:
        ctx.add_step("execution_collect", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", result.height)

    return list(result.iter_rows())
