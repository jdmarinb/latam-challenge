import pytest
import json

# Imports for Time implementation
from src.q3_time import (
    mention_extractor as me_time,
    mention_counter as mc_time,
    get_top_k as gk_time,
    q3_time,
)

# Imports for Memory implementation
from src.q3_memory import (
    mention_extractor as me_mem,
    mention_counter as mc_mem,
    get_top_k as gk_mem,
    q3_memory,
)

# --- 1. Configuration & Scenarios ---

TARGET_FUNCS = [
    # Time (Polars)
    ("time_mention_extractor", me_time),
    ("time_mention_counter", mc_time),
    ("time_get_top_k", lambda lf: gk_time(lf, 2)),
    ("time_q3", q3_time),
    # Memory (Functional)
    ("mem_mention_extractor", me_mem),
    ("mem_mention_counter", mc_mem),
    ("mem_get_top_k", lambda c: gk_mem(c, 2)),
    ("mem_q3", q3_memory),
]

TEST_SCENARIOS = {
    "happy_path": {
        "data": [
            {"mentionedUsers": [{"username": "UserA"}, {"username": "userb"}]},
            {"mentionedUsers": [{"username": "UserA"}]},
        ],
        "validators": {
            "time_mention_extractor": lambda res: res.collect().height == 3,
            "time_mention_counter": lambda res: res.collect().height == 2,
            "time_get_top_k": lambda res: res.collect().height == 2,
            "time_q3": lambda res: res[0] == ("usera", 2),
            "mem_mention_extractor": lambda res: len(list(res)) == 2,
            "mem_mention_counter": lambda res: len(res) == 2,
            "mem_get_top_k": lambda res: len(res) == 2,
            "mem_q3": lambda res: res[0] == ("usera", 2),
        },
    },
    "case_insensitivity": {
        "data": [
            {"mentionedUsers": [{"username": "LATAM"}, {"username": "latam"}]},
        ],
        "validators": {
            "time_mention_extractor": lambda res: res.collect()["username"][0]
            == "latam",
            "time_mention_counter": lambda res: res.collect().height == 1,
            "time_get_top_k": lambda res: res.collect().height == 1,
            "time_q3": lambda res: res[0] == ("latam", 2),
            "mem_mention_extractor": lambda res: list(res)[0][0] == "latam",
            "mem_mention_counter": lambda res: len(res) == 1,
            "mem_get_top_k": lambda res: len(res) == 1,
            "mem_q3": lambda res: res[0] == ("latam", 2),
        },
    },
    "null_and_empty": {
        "data": [
            {"mentionedUsers": None},
            {"mentionedUsers": []},
            {"mentionedUsers": [{"username": ""}, {"username": None}]},
        ],
        "validators": {
            "time_mention_extractor": lambda res: res.collect().height == 0,
            "time_mention_counter": lambda res: res.collect().height == 0,
            "time_get_top_k": lambda res: res.collect().height == 0,
            "time_q3": lambda res: res == [],
            "mem_mention_extractor": lambda res: all(len(m) == 0 for m in list(res)),
            "mem_mention_counter": lambda res: len(res) == 0,
            "mem_get_top_k": lambda res: len(res) == 0,
            "mem_q3": lambda res: res == [],
        },
    },
    "tie_breaking": {
        "data": [
            {"mentionedUsers": [{"username": "userB"}, {"username": "userA"}]},
        ],
        "validators": {
            "time_mention_extractor": lambda res: res.collect().height == 2,
            "time_mention_counter": lambda res: res.collect().height == 2,
            "time_get_top_k": lambda res: res.collect().height == 2,
            "time_q3": lambda res: res[0][0] == "usera" and res[1][0] == "userb",
            "mem_mention_extractor": lambda res: len(list(res)[0]) == 2,
            "mem_mention_counter": lambda res: len(res) == 2,
            "mem_get_top_k": lambda res: len(res) == 2,
            "mem_q3": lambda res: res[0][0] == "usera" and res[1][0] == "userb",
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
def test_q3_engine(json_factory, func_name, func_impl, scenario_name):
    config = TEST_SCENARIOS[scenario_name]
    validator = config["validators"].get(func_name)

    if not validator:
        pytest.skip(f"No validator for {func_name} in {scenario_name}")

    file_path = json_factory(f"{func_name}_{scenario_name}.json", config["data"])

    # Specific logic for components
    if "extractor" in func_name:
        if "time" in func_name:
            from src.common.utils import read_polars

            input_data = read_polars(file_path)
        else:
            input_data = file_path
        result = func_impl(input_data)

    elif "counter" in func_name:
        if "time" in func_name:
            from src.common.utils import read_polars

            lf = read_polars(file_path)
            input_data = me_time(lf)
        else:
            input_data = me_mem(file_path)
        result = func_impl(input_data)

    elif "get_top_k" in func_name:
        if "time" in func_name:
            from src.common.utils import read_polars

            lf = read_polars(file_path)
            counts_lf = mc_time(me_time(lf))
            result = func_impl(counts_lf)
        else:
            counter = mc_mem(me_mem(file_path))
            result = func_impl(counter)

    else:  # q3_time, q3_memory
        result = func_impl(file_path)

    assert validator(result)
