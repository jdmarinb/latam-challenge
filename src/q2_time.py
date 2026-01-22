import time
import polars as pl
from src.common.utils import read_polars as extractor
from src.common.logger import canonical_logger

# Modular Functional Blocks (KISS + Type Hints + Docstrings)


def emoji_extractor(lf: pl.LazyFrame, regex: str) -> pl.LazyFrame:
    """Extracts all emojis from content using a vectorized regex."""
    return (
        lf.select(pl.col("content").str.extract_all(regex).alias("emoji"))
        .explode("emoji")
        .filter(pl.col("emoji").is_not_null())
    )


def emoji_counter(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Counts occurrences of each emoji."""
    return lf.group_by("emoji").len()


def get_top_k(lf: pl.LazyFrame, k: int) -> pl.LazyFrame:
    """Selects the top k emojis based on frequency."""
    return lf.top_k(k, by="len")


@canonical_logger(event_name="q2_time_execution")
def q2_time(file_path: str, ctx=None) -> list[tuple[str, int]]:
    """
    Finds the top 10 most used emojis across all tweets.
    Uses Native Typing (Polars Schema) for ultra-fast validation and Canonical Logging.
    """
    if ctx:
        ctx.add_context(file_path=file_path)

    # Optimized regex for capturing emojis including ZWJ sequences and skin modifiers
    emoji_regex = r"(?:[\U0001f1e6-\U0001f1ff]{2}|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?:\p{EMod}|\ufe0f\u200d[\p{Emoji_Presentation}\p{Extended_Pictographic}])*+)"

    # Orchestrated pipeline
    t0 = time.perf_counter()
    query = (
        extractor(file_path)
        .pipe(emoji_extractor, regex=emoji_regex)
        .pipe(emoji_counter)
        .pipe(get_top_k, k=10)
        .sort([pl.col("len"), pl.col("emoji")], descending=[True, False])
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
