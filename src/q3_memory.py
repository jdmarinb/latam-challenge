from collections import Counter
from functools import reduce
from src.common.utils import read_streaming_orjson


def q3_memory(file_path: str) -> list[tuple[str, int]]:
    """
    Counts the top 10 most mentioned users using a memory-efficient functional pipeline.
    Uses orjson streaming and reduce with Counter.update.
    """

    # 1. Pipeline: Stream -> Extract Usernames -> Flatten -> Update Counter
    # Extract only the username from each mentioned user object
    user_mentions_stream = map(
        lambda t: [
            u.get("username").lower()
            for u in t.get("mentionedUsers", [])
            if u.get("username")
        ],
        read_streaming_orjson(file_path),
    )

    # 2. Reduce: Flatten the stream of lists and accumulate counts
    total_counts = reduce(
        lambda acc, usernames: (acc.update(usernames), acc)[1],
        user_mentions_stream,
        Counter(),
    )

    # 3. Result: Sort by count (desc) and username (asc) for deterministic tie-breaking
    return sorted(total_counts.items(), key=lambda x: (-x[1], x[0]))[:10]
