import polars as pl
from src.common.utils import twitter_schema


def q3_time(file_path: str) -> list[tuple[str, int]]:
    """
    Computes the top 10 mentioned users using a clean LazyFrame query.
    Optimized with top_k for performance.
    """
    query = (
        # Read JSON data as a LazyFrame
        pl.scan_ndjson(file_path, schema=twitter_schema)
        # Extract the username field from the mentionedUsers list
        .select(
            pl.col("mentionedUsers")
            .explode()
            .struct.field("username")
            .str.to_lowercase()
            .alias("username")
        )
        # Filter out invalid or empty usernames
        .filter(pl.col("username").is_not_null() & (pl.col("username") != ""))
        # Aggregate counts by username
        .group_by("username")
        .len()
        # Get the top 10 results efficiently using top_k
        .top_k(10, by="len")
        # Apply final sorting for consistent output
        .sort(["len", "username"], descending=[True, False])
    )

    # Collect the results and transform to list of tuples
    return list(query.collect().iter_rows())
