from datetime import datetime
from collections import Counter
from functools import reduce
from src.common.utils import read_streaming_orjson


def q1_memory(file_path: str) -> list[tuple[datetime.date, str]]:
    """
    Identifies the top 10 dates with the most tweets and their most active user.
    Optimized for memory using orjson streaming and functional pipelines.
    """

    # 1. First Pass: Count total tweets per date to identify the Top 10
    date_counts = reduce(
        lambda acc, date: (acc.update([date]), acc)[1],
        map(lambda t: t.get("date", "")[:10], read_streaming_orjson(file_path)),
        Counter(),
    )

    top_10_dates = [d for d, _ in date_counts.most_common(10)]
    target_dates = frozenset(top_10_dates)

    # 2. Second Pass: Count user activity only for the target dates
    # We use a reducer to build the map of {(date, user): count}
    user_date_counts = reduce(
        lambda acc, pair: (acc.update([pair]), acc)[1],
        filter(
            lambda p: p[0] in target_dates,
            map(
                lambda t: (t.get("date", "")[:10], t.get("user", {}).get("username")),
                read_streaming_orjson(file_path),
            ),
        ),
        Counter(),
    )

    # 3. Aggregation: Determine the best user for each top date with tie-breaking
    # Structure: {date: (best_user, max_count)}
    best_users = reduce(
        lambda acc, item: (
            lambda d, u, c: (
                (
                    acc.update({d: (u, c)})
                    if c > acc.get(d, (None, -1))[1]
                    or (
                        c == acc.get(d, (None, -1))[1] and u < acc.get(d, (None, -1))[0]
                    )
                    else None
                ),
                acc,
            )[1]
        )(item[0][0], item[0][1], item[1]),
        user_date_counts.items(),
        {},
    )

    # 4. Final Formatting: Order by original tweet volume
    return [
        (datetime.strptime(d, "%Y-%m-%d").date(), best_users[d][0])
        for d in top_10_dates
        if d in best_users
    ]
