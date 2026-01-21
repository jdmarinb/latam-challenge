from functools import reduce
from collections import Counter
from src.common.utils import read_orjson as extractor


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


def q3_memory(file_path: str) -> list[tuple[str, int]]:
    """
    Counts the top 10 most mentioned users using a memory-efficient functional pipeline.
    Uses orjson streaming and reduce with Counter.update.
    """
    # Define orchestrated pipeline steps
    pipeline = [
        # Step 1: Extract mentions from stream
        lambda _: mention_extractor(file_path),
        # Step 2: Aggregate counts
        lambda mention_stream: mention_counter(mention_stream),
        # Step 3: Get Top 10 with deterministic tie-breaking
        lambda counter: get_top_k(counter, 10),
    ]

    # Execute the orchestrated pipeline
    return reduce(lambda val, step: step(val), pipeline, None)
