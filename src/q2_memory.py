from collections import Counter
from functools import reduce
import re
from src.common.utils import read_streaming_orjson


def q2_memory(file_path: str) -> list[tuple[str, int]]:
    """
    Counts the top 10 most used emojis using a memory-efficient functional pipeline.
    Uses orjson streaming and reduce with Counter.update for efficiency.
    """
    # Regex for capturing emojis (including ZWJ and modifiers)
    emoji_pattern = re.compile(
        r"("
        r"[\U0001f1e6-\U0001f1ff]{2}|"
        r"[\U0001f300-\U0001f9ff\u2600-\u26ff\u2700-\u27bf]"
        r"(?:[\ufe0f\u200d\U0001f3fb-\U0001f3ff]+"
        r"[\U0001f300-\U0001f9ff\u2600-\u26ff\u2700-\u27bf])*"
        r")"
    )

    # 1. Pipeline: Stream -> Extract -> Update Counter
    # We use a reducer that directly calls Counter.update for each list of emojis found
    total_counts = reduce(
        lambda acc, emojis: (acc.update(emojis), acc)[1],
        map(
            lambda t: emoji_pattern.findall(t.get("content", "")),
            read_streaming_orjson(file_path),
        ),
        Counter(),
    )

    # 2. Result: Sort by count (desc) and emoji (asc) for deterministic results
    return sorted(total_counts.items(), key=lambda x: (-x[1], x[0]))[:10]
