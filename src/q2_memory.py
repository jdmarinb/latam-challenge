import re
from functools import reduce
from collections import Counter
from src.common.utils import read_streaming_orjson as extractor


# Modular Functional Blocks (KISS)
def get_top_k(counters, k):
    return sorted(counters.items(), key=lambda x: (-x[1], x[0]))[:k]


def emoji_extractor(file_path, pattern):
    return map(
        lambda t: pattern.findall(t.get("content", "")),
        extractor(file_path),
    )


def emoji_counter(emoji_stream):
    return reduce(
        lambda acc, emojis: (acc.update(emojis), acc)[1],
        emoji_stream,
        Counter(),
    )


def q2_memory(file_path: str) -> list[tuple[str, int]]:
    """
    Counts the top 10 most used emojis using a memory-efficient functional pipeline.
    Uses orjson streaming and reduce with Counter.update for efficiency.
    """
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
    pipeline = [
        # Step 1: Extract emojis from stream
        lambda _: emoji_extractor(file_path, emoji_regex),
        # Step 2: Aggregate counts
        lambda emoji_stream: emoji_counter(emoji_stream),
        # Step 3: Get Top 10 with deterministic tie-breaking
        lambda counter: get_top_k(counter, 10),
    ]

    # Execute the orchestrated pipeline
    return reduce(lambda val, step: step(val), pipeline, None)
