import time
from functools import reduce
from collections import Counter
from src.common.utils import read_orjson as extractor
from src.common.logger import canonical_logger


# Modular Functional Blocks (KISS)
get_top_k = lambda counters, k: sorted(counters.items(), key=lambda x: (-x[1], x[0]))[
    :k
]


mention_extractor = lambda file_path: map(
    lambda t: [
        u.get("username").lower()
        for u in (t.get("mentionedUsers") or [])
        if u.get("username")
    ],
    extractor(file_path),
)


mention_counter = lambda mention_stream: reduce(
    lambda acc, usernames: (acc.update(usernames), acc)[1],
    mention_stream,
    Counter(),
)


@canonical_logger(event_name="q3_memory_execution")
def q3_memory(file_path: str, ctx=None) -> list[tuple[str, int]]:
    """
    Counts the top 10 most mentioned users using a memory-efficient functional pipeline.
    Uses orjson streaming and reduce with Counter.update.
    """
    if ctx:
        ctx.add_context(file_path=file_path)

    # Define orchestrated pipeline steps
    t0 = time.perf_counter()
    mention_stream = mention_extractor(file_path)
    if ctx:
        ctx.add_step(
            "create_extraction_stream", round((time.perf_counter() - t0) * 1000, 4)
        )

    t0 = time.perf_counter()
    counter = mention_counter(mention_stream)
    if ctx:
        ctx.add_step("aggregate_counts", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("unique_mentions", len(counter))

    t0 = time.perf_counter()
    result = get_top_k(counter, 10)
    if ctx:
        ctx.add_step("get_top_10", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", len(result))

    return result
