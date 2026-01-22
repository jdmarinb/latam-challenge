import time
from functools import reduce
from collections import Counter
from collections.abc import Iterable
from src.common.utils import read_msgspec, mention_decoder
from src.common.logger import canonical_logger


# Modular Functional Blocks (KISS + Type Hints + Docstrings)


def get_top_k(counters: Counter, k: int) -> list[tuple[str, int]]:
    """Sorts and returns the top k elements from a Counter."""
    return sorted(counters.items(), key=lambda x: (-x[1], x[0]))[:k]


def mention_extractor(file_path: str) -> Iterable[list[str]]:
    """Yields lists of mentioned usernames from each tweet using msgspec."""
    return map(
        lambda t: [m.username.lower() for m in (t.mentionedUsers or [])]
        if t.mentionedUsers
        else [],
        read_msgspec(file_path, decoder=mention_decoder),
    )


def mention_counter(mention_stream: Iterable[list[str]]) -> Counter:
    """Aggregates all mentioned usernames into a single Counter."""
    return reduce(
        lambda acc, usernames: (acc.update(usernames), acc)[1],
        mention_stream,
        Counter(),
    )


@canonical_logger(event_name="q3_memory_execution")
def q3_memory(file_path: str, ctx=None) -> list[tuple[str, int]]:
    """
    Counts the top 10 most mentioned users using a memory-efficient functional pipeline.
    Uses msgspec for ultra-fast mention extraction and Canonical Logging for observability.
    """
    if ctx:
        ctx.add_context(file_path=file_path)

    # Define orchestrated pipeline steps
    t0 = time.perf_counter()
    stream = mention_extractor(file_path)
    if ctx:
        ctx.add_step(
            "create_extraction_stream", round((time.perf_counter() - t0) * 1000, 4)
        )

    t0 = time.perf_counter()
    counter = mention_counter(stream)
    if ctx:
        ctx.add_step("aggregate_counts", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("unique_mentions", len(counter))

    t0 = time.perf_counter()
    result = get_top_k(counter, 10)
    if ctx:
        ctx.add_step("get_top_10", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", len(result))

    return result
