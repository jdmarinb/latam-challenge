import time
import polars as pl
from datetime import datetime
from src.common.utils import read_polars as extractor
from src.common.logger import canonical_logger

# Modular Functional Blocks returning LazyFrames (Optimized for Time)
date_counter = lambda file_path: (
    extractor(file_path)
    .with_columns(
        pl.col("date")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
        .dt.date()
        .alias("date")
    )
    .filter(pl.col("date").is_not_null())  # Strategy 5 & 2 (Null handling)
    .group_by("date")
    .len()
)

get_top_k = lambda lf, k: lf.top_k(k, by="len").select("date")

user_date_counter = lambda file_path, top_dates_lf: (
    extractor(file_path)
    .with_columns(
        pl.col("date")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
        .dt.date()
        .alias("date"),
        pl.col("user").struct.field("username").alias("username"),
    )
    .join(top_dates_lf, on="date")
    .filter(pl.col("username").is_not_null())  # Strategy 2: Schema consistency
    .group_by("date", "username")
    .len()
)

user_ranker = lambda lf: lf.group_by("date").agg(
    pl.col("username")
    .sort_by(["len", "username"], descending=[True, False])
    .first()
    .alias("top_user"),
    pl.col("len").sum().alias("day_total_tweets"),
)


@canonical_logger(event_name="q1_time_execution")
def q1_time(file_path: str, ctx=None) -> list[tuple[datetime.date, str]]:
    """
    Computes top 10 dates and their most active user using an optimized Lazy pipeline.
    Errors are handled natively within the Polars expression tree (Vectorized paradigm).
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

    # 4. SINGLE EXECUTION
    t0 = time.perf_counter()
    result = query.collect()
    if ctx:
        ctx.add_step("execution_collect", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", result.height)

    return list(result.iter_rows())
