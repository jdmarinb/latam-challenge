import polars as pl
from src.common.utils import twitter_schema


def q2_time(file_path: str) -> list[tuple[str, int]]:
    """
    Finds the top 10 most used emojis across all tweets.
    Uses regex for emoji extraction and top_k for performance.
    """
    # Optimized regex for capturing emojis including ZWJ sequences and skin modifiers
    emoji_regex = r"(?:[\U0001f1e6-\U0001f1ff]{2}|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?:\p{EMod}|\ufe0f\u200d[\p{Emoji_Presentation}\p{Extended_Pictographic}])*+)"

    query = (
        pl.scan_ndjson(file_path, schema=twitter_schema)
        # Extract and explode emojis into separate rows
        .select(pl.col("content").str.extract_all(emoji_regex).alias("emoji"))
        .explode("emoji")
        # Filter out tweets without emojis
        .filter(pl.col("emoji").is_not_null())
        # Aggregate counts
        .group_by("emoji")
        .len()
        # Efficiently retrieve top 10 results
        .top_k(10, by="len")
        # Final sort for deterministic results
        .sort([pl.col("len"), pl.col("emoji")], descending=[True, False])
    )

    # Collect and transform to list of tuples
    return list(query.collect().iter_rows())
