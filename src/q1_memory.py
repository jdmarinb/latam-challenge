from datetime import datetime
from collections import Counter
from functools import reduce
from src.common.utils import read_orjson as extractor


# Modular Functional Blocks (KISS)
get_top_k = lambda counters, k: [d for d, _ in counters.most_common(k)]


date_counter = lambda file_path: reduce(
    lambda acc, date: (acc.update([date]), acc)[1],
    map(lambda t: t.get("date", "")[:10], extractor(file_path)),
    Counter(),
)


user_date_counter = lambda file_path, target_dates: reduce(
    lambda acc, pair: (acc.update([pair]), acc)[1],
    filter(
        lambda p: p[0] in target_dates,
        map(
            lambda t: (t.get("date", "")[:10], t.get("user", {}).get("username")),
            extractor(file_path),
        ),
    ),
    Counter(),
)


user_ranker = lambda user_date_counts: reduce(
    lambda acc, item: (
        lambda d, u, c: (
            (
                acc.update({d: (u, c)})
                if c > acc.get(d, (None, -1))[1]
                or (c == acc.get(d, (None, -1))[1] and u < acc.get(d, (None, -1))[0])
                else None
            ),
            acc,
        )[1]
    )(item[0][0], item[0][1], item[1]),
    user_date_counts.items(),
    {},
)


def q1_memory(file_path: str) -> list[tuple[datetime.date, str]]:
    """
    Identifies the top 10 dates with the most tweets and their most active user.
    Uses functional orchestration with reduce for a clean, modular pipeline.
    """

    # Define the orchestrated pipeline steps
    pipeline = [
        # Step 1: Identify top 10 dates
        lambda _: get_top_k(date_counter(file_path), 10),
        # Step 2: Get user counts for those dates and find the best users
        # This step returns a tuple of (top_dates, best_users_map)
        lambda dates: (
            dates,
            user_ranker(user_date_counter(file_path, frozenset(dates))),
        ),
        # Step 3: Format the results
        lambda state: [
            (datetime.strptime(d, "%Y-%m-%d").date(), state[1][d][0])
            for d in state[0]
            if d in state[1]
        ],
    ]

    # Execute the orchestrated pipeline
    return reduce(lambda val, step: step(val), pipeline, None)
