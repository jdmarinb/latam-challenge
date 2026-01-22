import time
import polars as pl
from datetime import date
from src.common.utils import read_polars as extractor
from src.common.logger import canonical_logger

# Modular Functional Blocks (KISS + Type Hints + Docstrings)


def date_counter(file_path: str) -> pl.LazyFrame:
    """Creates a LazyFrame pipeline to count tweets per date."""
    return (
        extractor(file_path)
        .with_columns(
            pl.col("date")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .dt.date()
            .alias("date")
        )
        .filter(pl.col("date").is_not_null())
        .group_by("date")
        .len()
    )


def get_top_k(lf: pl.LazyFrame, k: int) -> pl.LazyFrame:
    """Adds top-k selection to the LazyFrame pipeline."""
    return lf.top_k(k, by="len").select("date")


def user_date_counter(file_path: str, top_dates_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Counts user activity for specific target dates."""
    return (
        extractor(file_path)
        .with_columns(
            pl.col("date")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .dt.date()
            .alias("date"),
            pl.col("user").struct.field("username").alias("username"),
        )
        .join(top_dates_lf, on="date")
        .filter(pl.col("username").is_not_null())
        .group_by("date", "username")
        .len()
    )


def user_ranker(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Ranks users per date to find the most active one."""
    return lf.group_by("date").agg(
        pl.col("username")
        .sort_by(["len", "username"], descending=[True, False])
        .first()
        .alias("top_user"),
        pl.col("len").sum().alias("day_total_tweets"),
    )


@canonical_logger(event_name="q1_time_execution")
def q1_time(file_path: str, ctx=None) -> list[tuple[date, str]]:
    """
    Computes top 10 dates and their most active user using an optimized Lazy pipeline.
    Uses Native Typing (Polars Schema) for ultra-fast validation and Canonical Logging.
    """
    if ctx:
        ctx.add_context(file_path=file_path)

    # 1. Plan for Top 10 Dates
    t0 = time.perf_counter()
    top_dates_lf = get_top_k(date_counter(file_path), 10)
    if ctx:
        ctx.add_step("plan_top_dates", round((time.perf_counter() - t0) * 1000, 4))

    # 2. Plan for User activity on those dates
    t0 = time.perf_counter()
    user_counts_lf = user_date_counter(file_path, top_dates_lf)
    if ctx:
        ctx.add_step("plan_user_counts", round((time.perf_counter() - t0) * 1000, 4))

    # 3. Plan for final ranking and sort
    t0 = time.perf_counter()
    query = (
        user_ranker(user_counts_lf)
        .sort(["day_total_tweets", "date"], descending=[True, True])
        .select("date", "top_user")
    )
    if ctx:
        ctx.add_step("plan_final_query", round((time.perf_counter() - t0) * 1000, 4))

    # 4. SINGLE EXECUTION (Validation happens here at Rust level via schema)
    t0 = time.perf_counter()
    result = query.collect()

    if ctx:
        ctx.add_step("execution_collect", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", result.height)

    return list(result.iter_rows())
