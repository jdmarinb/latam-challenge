import re
import time
from functools import reduce
from collections import Counter
from collections.abc import Iterable
from src.common.utils import read_msgspec, content_decoder
from src.common.logger import canonical_logger


# Modular Functional Blocks (KISS + Type Hints + Docstrings)


def get_top_k(counters: Counter, k: int) -> list[tuple[str, int]]:
    """Sorts and returns the top k elements from a Counter."""
    return sorted(counters.items(), key=lambda x: (-x[1], x[0]))[:k]


def emoji_extractor(file_path: str, pattern: re.Pattern) -> Iterable[list[str]]:
    """Yields lists of emojis extracted from each tweet content using msgspec."""
    return map(
        lambda t: pattern.findall(t.content),
        read_msgspec(file_path, decoder=content_decoder),
    )


def emoji_counter(emoji_stream: Iterable[list[str]]) -> Counter:
    """Aggregates all extracted emojis into a single Counter."""
    return reduce(
        lambda acc, emojis: (acc.update(emojis), acc)[1],
        emoji_stream,
        Counter(),
    )


@canonical_logger(event_name="q2_memory_execution")
def q2_memory(file_path: str, ctx=None) -> list[tuple[str, int]]:
    """
    Counts the top 10 most used emojis using a memory-efficient functional pipeline.
    Uses msgspec for fast content extraction and Canonical Logging for observability.
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
    stream = emoji_extractor(file_path, emoji_regex)
    if ctx:
        ctx.add_step(
            "create_extraction_stream", round((time.perf_counter() - t0) * 1000, 4)
        )

    t0 = time.perf_counter()
    counter = emoji_counter(stream)
    if ctx:
        ctx.add_step("aggregate_counts", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("unique_emojis", len(counter))

    t0 = time.perf_counter()
    result = get_top_k(counter, 10)
    if ctx:
        ctx.add_step("get_top_10", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", len(result))

    return result
