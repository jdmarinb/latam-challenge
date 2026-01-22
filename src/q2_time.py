import time
import polars as pl
from src.common.utils import read_polars as extractor
from src.common.logger import canonical_logger


# Modular Functional Blocks returning LazyFrames (Optimized for Time)
emoji_extractor = lambda lf, regex: (
    lf.select(pl.col("content").str.extract_all(regex).alias("emoji"))
    .explode("emoji")
    .filter(pl.col("emoji").is_not_null())
)

emoji_counter = lambda lf: lf.group_by("emoji").len()

get_top_k = lambda lf, k: lf.top_k(k, by="len")


@canonical_logger(event_name="q2_time_execution")
def q2_time(file_path: str, ctx=None) -> list[tuple[str, int]]:
    """
    Finds the top 10 most used emojis across all tweets.
    Uses regex for emoji extraction and top_k for performance.
    Errors are handled natively within the Polars expression tree (Vectorized).
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

    # SINGLE EXECUTION
    t0 = time.perf_counter()
    result = query.collect()
    if ctx:
        ctx.add_step("execution_collect", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", result.height)

    return list(result.iter_rows())
