import re
import time
from functools import reduce
from collections import Counter
from src.common.utils import read_orjson as extractor
from src.common.logger import canonical_logger


# Modular Functional Blocks (KISS)
get_top_k = lambda counters, k: sorted(counters.items(), key=lambda x: (-x[1], x[0]))[
    :k
]


emoji_extractor = lambda file_path, pattern: map(
    lambda t: pattern.findall(t.get("content", "")),
    extractor(file_path),
)


emoji_counter = lambda emoji_stream: reduce(
    lambda acc, emojis: (acc.update(emojis), acc)[1],
    emoji_stream,
    Counter(),
)


@canonical_logger(event_name="q2_memory_execution")
def q2_memory(file_path: str, ctx=None) -> list[tuple[str, int]]:
    """
    Counts the top 10 most used emojis using a memory-efficient functional pipeline.
    Uses orjson streaming and reduce with Counter.update for efficiency.
    """
    if ctx:
        ctx.add_context(file_path=file_path)

    # Regex for capturing emojis (including ZWJ and modifiers)
    emoji_regex = re.compile(
        r"("
        r"[\U0001f1e6-\U0001f1ff]{2}|"
        r"[\U0001f300-\U0001f9ff\u2600-\u26ff\u2700-\u27bf]"
        r"(?:[\ufe0f\u200d\U0001f3fb-\U0001f3ff]+"
        r"[\U0001f300-\U0001f9ff\u2600-\u26ff\u2700-\u27bf])*"
        r")"
    )

    # Define orchestrated pipeline steps
    t0 = time.perf_counter()
    emoji_stream = emoji_extractor(file_path, emoji_regex)
    if ctx:
        ctx.add_step(
            "create_extraction_stream", round((time.perf_counter() - t0) * 1000, 4)
        )

    t0 = time.perf_counter()
    counter = emoji_counter(emoji_stream)
    if ctx:
        ctx.add_step("aggregate_counts", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("unique_emojis", len(counter))

    t0 = time.perf_counter()
    result = get_top_k(counter, 10)
    if ctx:
        ctx.add_step("get_top_10", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", len(result))

    return result
