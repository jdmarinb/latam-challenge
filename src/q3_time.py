import time
import polars as pl
from src.common.utils import read_polars as extractor
from src.common.logger import canonical_logger


# Modular Functional Blocks returning LazyFrames (Optimized for Time)
mention_extractor = lambda lf: (
    lf.select(
        pl.col("mentionedUsers")
        .explode()
        .struct.field("username")
        .str.to_lowercase()
        .alias("username")
    ).filter(pl.col("username").is_not_null() & (pl.col("username") != ""))
)

mention_counter = lambda lf: lf.group_by("username").len()

get_top_k = lambda lf, k: lf.top_k(k, by="len")


@canonical_logger(event_name="q3_time_execution")
def q3_time(file_path: str, ctx=None) -> list[tuple[str, int]]:
    """
    Computes the top 10 mentioned users using a clean LazyFrame query.
    Errors are handled natively within the Polars expression tree (Vectorized).
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

    # SINGLE EXECUTION
    t0 = time.perf_counter()
    result = query.collect()
    if ctx:
        ctx.add_step("execution_collect", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", result.height)

    return list(result.iter_rows())
