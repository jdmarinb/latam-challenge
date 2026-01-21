from datetime import datetime
import polars as pl
from src.common.utils import twitter_schema


def q1_time(file_path: str) -> list[tuple[datetime.date, str]]:
    """
    Computes the top 10 dates with most tweets and their most active user.
    Uses an optimized LazyFrame pipeline with Predicate Pushdown.
    """
    # Define the base LazyFrame and normalize common columns
    lf = (
        pl.scan_ndjson(file_path, schema=twitter_schema)
        .with_columns(
            pl.col("date")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .dt.date()
            .alias("date"),
            pl.col("user").struct.field("username").alias("username"),
        )
        .filter(pl.col("date").is_not_null() & pl.col("username").is_not_null())
    )

    # Calculate Top 10 Days
    # Optimized using top_k (O(N))
    top_days_lf = lf.group_by("date").len().top_k(10, by="len")

    # Join and process to find the top user for those specific days
    query = (
        lf
        # Filter rows by joining with top_days_lf
        .join(top_days_lf, on="date")
        # Count tweets per user on those days
        .group_by("date", "username")
        .len()
        # Determine the user with the most tweets within each group
        .group_by("date")
        .agg(
            pl.col("username")
            .sort_by(["len", "username"], descending=[True, False])
            .first()
            .alias("top_user"),
            pl.col("len").sum().alias("day_total_tweets"),
        )
        # Final ordering by total volume and date
        .sort(["day_total_tweets", "date"], descending=[True, True])
        .select("date", "top_user")
    )

    # Collect and transform results
    return list(query.collect().iter_rows())
