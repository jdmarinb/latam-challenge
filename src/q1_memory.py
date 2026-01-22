import time
from datetime import datetime
from collections import Counter
from functools import reduce
from src.common.utils import read_orjson as extractor
from src.common.logger import canonical_logger


# Modular Functional Blocks (KISS)
get_top_k = lambda counters, k: [d for d, _ in counters.most_common(k)]


date_counter = lambda file_path: reduce(
    lambda acc, date: (acc.update([date]), acc)[1],
    filter(
        None, map(lambda t: t.get("date", "")[:10], extractor(file_path))
    ),  # Strategy 5 & 2
    Counter(),
)


user_date_counter = (
    lambda file_path, target_dates: reduce(
        lambda acc, pair: (acc.update([pair]), acc)[1],
        filter(
            lambda p: p[0] in target_dates and p[1],  # Strategy 2: Schema inconsistency
            map(
                lambda t: (t.get("date", "")[:10], t.get("user", {}).get("username")),
                extractor(file_path),
            ),
        ),
        Counter(),
    )
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


@canonical_logger(event_name="q1_memory_execution")
def q1_memory(file_path: str, ctx=None) -> list[tuple[datetime.date, str]]:
    """
    Identifies the top 10 dates with the most tweets and their most active user.
    Errors are handled using functional composition (Paradigm Integrity).
    Monadic handling is simulated by map/filter chains that ignore invalid records.
    """
    if ctx:
        ctx.add_context(file_path=file_path)

    # 1. Step 1: Identify top 10 dates
    t0 = time.perf_counter()
    counts = date_counter(file_path)
    top_dates = get_top_k(counts, 10)
    if ctx:
        ctx.add_step("identify_top_dates", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("total_dates", len(counts))

    # 2. Step 2: Get user counts for those dates
    t0 = time.perf_counter()
    user_date_counts = user_date_counter(file_path, frozenset(top_dates))
    if ctx:
        ctx.add_step(
            "count_users_for_top_dates", round((time.perf_counter() - t0) * 1000, 4)
        )

    # 3. Step 3: Find best users
    t0 = time.perf_counter()
    best_users_map = user_ranker(user_date_counts)
    if ctx:
        ctx.add_step("rank_users", round((time.perf_counter() - t0) * 1000, 4))

    # 4. Step 4: Format results
    t0 = time.perf_counter()
    result = [
        (datetime.strptime(d, "%Y-%m-%d").date(), best_users_map[d][0])
        for d in top_dates
        if d in best_users_map
    ]
    if ctx:
        ctx.add_step("format_results", round((time.perf_counter() - t0) * 1000, 4))
        ctx.add_metric("output_rows", len(result))

    return result
