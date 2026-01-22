import time
from datetime import datetime
from collections import Counter
from functools import reduce
from src.common.utils import read_msgspec, tweet_decoder
from src.common.logger import canonical_logger


# Modular Functional Blocks (KISS + Type Hints + Docstrings)


def get_top_k(counters: Counter, k: int) -> list[str]:
    """Extracts top k keys from a Counter object based on frequency."""
    return [d for d, _ in counters.most_common(k)]


def date_counter(file_path: str) -> Counter:
    """Counts tweet occurrences per date using memory-efficient streaming with msgspec validation."""
    return reduce(
        lambda acc, t: (acc.update([t.date[:10]]), acc)[1],
        read_msgspec(file_path, decoder=tweet_decoder),
        Counter(),
    )


def user_date_counter(file_path: str, target_dates: frozenset[str]) -> Counter:
    """Counts user activity for specific target dates using validated msgspec objects."""
    return reduce(
        lambda acc, t: (
            (acc.update([(t.date[:10], t.user.username)]), acc)[1]
            if t.date[:10] in target_dates
            else acc
        ),
        read_msgspec(file_path, decoder=tweet_decoder),
        Counter(),
    )


def user_ranker(user_date_counts: Counter) -> dict:
    """Ranks users per date to find the most active one."""
    return reduce(
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


@canonical_logger(event_name="q1_memory_execution")
def q1_memory(file_path: str, ctx=None) -> list[tuple[datetime.date, str]]:
    """
    Identifies the top 10 dates with the most tweets and their most active user.
    Uses msgspec for ultra-fast type validation and Canonical Logging for observability.
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
