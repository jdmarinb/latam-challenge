import polars as pl
from src.common.utils import read_polars as extractor


# Modular Functional Blocks returning LazyFrames (Optimized for Time)
def mention_extractor(lf):
    return lf.select(
        pl.col("mentionedUsers")
        .explode()
        .struct.field("username")
        .str.to_lowercase()
        .alias("username")
    ).filter(pl.col("username").is_not_null() & (pl.col("username") != ""))


def mention_counter(lf):
    return lf.group_by("username").len()


def get_top_k(lf, k):
    return lf.top_k(k, by="len")


def q3_time(file_path: str) -> list[tuple[str, int]]:
    """
    Computes the top 10 mentioned users using a clean LazyFrame query.
    Optimized with top_k for performance.
    """
    # Orchestrated pipeline
    query = (
        extractor(file_path)
        .pipe(mention_extractor)
        .pipe(mention_counter)
        .pipe(get_top_k, k=10)
        .sort(["len", "username"], descending=[True, False])
    )

    # SINGLE EXECUTION
    result = query.collect()
    return list(result.iter_rows())
