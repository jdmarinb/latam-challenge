import pytest
import polars as pl
import json
from datetime import date
from collections import Counter

# Imports for Time implementation
from src.q1_time import (
    date_counter as dc_time,
    get_top_k as gk_time,
    user_date_counter as udc_time,
    user_ranker as ur_time,
    q1_time,
)

# Imports for Memory implementation
from src.q1_memory import (
    date_counter as dc_mem,
    get_top_k as gk_mem,
    user_date_counter as udc_mem,
    user_ranker as ur_mem,
    q1_memory,
)

# --- 1. Configuration & Scenarios ---

TARGET_FUNCS = [
    # Time (Polars)
    ("time_date_counter", dc_time),
    ("time_get_top_k", lambda lf: gk_time(lf, 2)),
    (
        "time_user_date_counter",
        lambda fp: udc_time(fp, pl.LazyFrame({"date": [date(2021, 2, 12)]})),
    ),
    ("time_user_ranker", ur_time),
    ("time_q1", q1_time),
    # Memory (Functional)
    ("mem_date_counter", dc_mem),
    ("mem_get_top_k", lambda c: gk_mem(c, 2)),
    ("mem_user_date_counter", lambda fp: udc_mem(fp, frozenset(["2021-02-12"]))),
    ("mem_user_ranker", ur_mem),
    ("mem_q1", q1_memory),
]

TEST_SCENARIOS = {
    "basic_flow": {
        "data": [
            {
                "date": "2021-02-12T10:00:00+00:00",
                "user": {"id": 1, "username": "user1"},
            },
            {
                "date": "2021-02-12T11:00:00+00:00",
                "user": {"id": 1, "username": "user1"},
            },
            {
                "date": "2021-02-12T12:00:00+00:00",
                "user": {"id": 2, "username": "user2"},
            },
            {
                "date": "2021-02-13T10:00:00+00:00",
                "user": {"id": 3, "username": "user3"},
            },
        ],
        "validators": {
            "time_date_counter": lambda res: res.collect().height == 2,
            "time_get_top_k": lambda res: res.collect()["date"][0] == date(2021, 2, 12),
            "time_user_date_counter": lambda res: res.collect().height == 2,
            "time_user_ranker": lambda res: res.collect().height == 2,
            "time_q1": lambda res: res[0] == (date(2021, 2, 12), "user1"),
            "mem_date_counter": lambda res: len(res) == 2 and res["2021-02-12"] == 3,
            "mem_get_top_k": lambda res: res[0] == "2021-02-12",
            "mem_user_date_counter": lambda res: len(res) == 2,
            "mem_user_ranker": lambda res: len(res) == 2,
            "mem_q1": lambda res: res[0] == (date(2021, 2, 12), "user1"),
        },
    },
    "tie_breaking": {
        "data": [
            {
                "date": "2021-02-12T10:00:00+00:00",
                "user": {"id": 2, "username": "userB"},
            },
            {
                "date": "2021-02-12T11:00:00+00:00",
                "user": {"id": 1, "username": "userA"},
            },
        ],
        "validators": {
            "time_date_counter": lambda res: res.collect().height == 1,
            "time_get_top_k": lambda res: res.collect().height == 1,
            "time_user_date_counter": lambda res: res.collect().height == 2,
            "time_user_ranker": lambda res: res.collect()["top_user"][0] == "userA",
            "time_q1": lambda res: res[0][1] == "userA",
            "mem_date_counter": lambda res: len(res) == 1,
            "mem_get_top_k": lambda res: len(res) == 1,
            "mem_user_date_counter": lambda res: len(res) == 2,
            "mem_user_ranker": lambda res: res["2021-02-12"][0] == "userA",
            "mem_q1": lambda res: res[0][1] == "userA",
        },
    },
    "empty_file": {
        "data": [],
        "validators": {
            "time_date_counter": lambda res: res.collect().height == 0,
            "time_get_top_k": lambda res: res.collect().height == 0,
            "time_user_date_counter": lambda res: res.collect().height == 0,
            "time_user_ranker": lambda res: res.collect().height == 0,
            "time_q1": lambda res: res == [],
            "mem_date_counter": lambda res: len(res) == 0,
            "mem_get_top_k": lambda res: len(res) == 0,
            "mem_user_date_counter": lambda res: len(res) == 0,
            "mem_user_ranker": lambda res: len(res) == 0,
            "mem_q1": lambda res: res == [],
        },
    },
}

# --- 2. Shared Fixtures ---


@pytest.fixture
def json_factory(tmp_path):
    def _create(filename, content):
        p = tmp_path / filename
        with open(p, "w", encoding="utf-8") as f:
            for item in content:
                f.write(json.dumps(item) + "\n")
        return str(p)

    return _create


# --- 3. The Driver Test Function ---


@pytest.mark.parametrize("func_name, func_impl", TARGET_FUNCS)
@pytest.mark.parametrize("scenario_name", TEST_SCENARIOS.keys())
def test_q1_engine(json_factory, func_name, func_impl, scenario_name):
    config = TEST_SCENARIOS[scenario_name]
    validator = config["validators"].get(func_name)

    if not validator:
        pytest.skip(f"No validator for {func_name} in {scenario_name}")

    file_path = json_factory(f"{func_name}_{scenario_name}.json", config["data"])

    # Specific logic for components that don't take a file path directly
    if "user_ranker" in func_name:
        if "time" in func_name:
            if scenario_name == "tie_breaking":
                input_data = pl.LazyFrame(
                    {
                        "date": [date(2021, 2, 12), date(2021, 2, 12)],
                        "username": ["userA", "userB"],
                        "len": [1, 1],
                    }
                )
            elif scenario_name == "basic_flow":
                input_data = pl.LazyFrame(
                    {
                        "date": [
                            date(2021, 2, 12),
                            date(2021, 2, 12),
                            date(2021, 2, 13),
                        ],
                        "username": ["user1", "user2", "user3"],
                        "len": [2, 1, 1],
                    }
                )
            else:
                input_data = pl.LazyFrame(
                    {"date": [], "username": [], "len": []},
                    schema={"date": pl.Date, "username": pl.String, "len": pl.UInt32},
                )
        else:  # memory
            if scenario_name == "tie_breaking":
                input_data = Counter(
                    {("2021-02-12", "userA"): 1, ("2021-02-12", "userB"): 1}
                )
            elif scenario_name == "basic_flow":
                input_data = Counter(
                    {
                        ("2021-02-12", "user1"): 2,
                        ("2021-02-12", "user2"): 1,
                        ("2021-02-13", "user3"): 1,
                    }
                )
            else:
                input_data = Counter()
        result = func_impl(input_data)

    elif "get_top_k" in func_name:
        if "time" in func_name:
            lf_counts = dc_time(file_path)
            result = func_impl(lf_counts)
        else:  # memory
            counts = dc_mem(file_path)
            result = func_impl(counts)

    else:  # q1_time, q1_memory, date_counter, user_date_counter
        result = func_impl(file_path)

    assert validator(result), f"Validator failed for {func_name} in {scenario_name}"
