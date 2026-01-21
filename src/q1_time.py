import polars as pl
from datetime import datetime
from src.common.utils import read_polars as extractor


# Modular Functional Blocks returning LazyFrames (Optimized for Time)
def date_counter(file_path):
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


def get_top_k(lf, k):
    return lf.top_k(k, by="len").select("date")


def user_date_counter(file_path, top_dates_lf):
    return (
        extractor(file_path)
        .with_columns(
            pl.col("date")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .dt.date()
            .alias("date"),
            pl.col("user").struct.field("username").alias("username"),
        )
        .join(
            top_dates_lf, on="date"
        )  # The join acts as a massive filter (Predicate Pushdown)
        .filter(pl.col("username").is_not_null())
        .group_by("date", "username")
        .len()
    )


def user_ranker(lf):
    return lf.group_by("date").agg(
        pl.col("username")
        .sort_by(["len", "username"], descending=[True, False])
        .first()
        .alias("top_user"),
        pl.col("len")
        .sum()
        .alias("day_total_tweets"),  # Re-calculate volume for final ordering
    )


def q1_time(file_path: str) -> list[tuple[datetime.date, str]]:
    """
    Computes top 10 dates and their most active user using an optimized Lazy pipeline.
    Maintains modularity for testability while keeping execution fast.
    """
    # 1. Plan for Top 10 Dates
    top_dates_lf = get_top_k(date_counter(file_path), 10)

    # 2. Plan for User activity on those dates
    user_counts_lf = user_date_counter(file_path, top_dates_lf)

    # 3. Plan for final ranking and sort
    query = (
        user_ranker(user_counts_lf)
        .sort(["day_total_tweets", "date"], descending=[True, True])
        .select("date", "top_user")
    )

    # 4. SINGLE EXECUTION: Polars optimizes the entire DAG here
    result = query.collect()

    return list(result.iter_rows())
