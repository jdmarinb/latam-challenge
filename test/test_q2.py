import pytest
import json
import re

# Imports for Time implementation
from src.q2_time import (
    emoji_extractor as ee_time,
    emoji_counter as ec_time,
    get_top_k as gk_time,
    q2_time,
)

# Imports for Memory implementation
from src.q2_memory import (
    emoji_extractor as ee_mem,
    emoji_counter as ec_mem,
    get_top_k as gk_mem,
    q2_memory,
)

# Shared Regex for testing components
TIME_REGEX = r"(?:[\U0001f1e6-\U0001f1ff]{2}|[\p{Emoji_Presentation}\p{Extended_Pictographic}](?:\p{EMod}|\ufe0f\u200d[\p{Emoji_Presentation}\p{Extended_Pictographic}])*+)"
MEM_REGEX = re.compile(
    r"("
    r"[\U0001f1e6-\U0001f1ff]{2}|"
    r"[\U0001f300-\U0001f9ff\u2600-\u26ff\u2700-\u27bf]"
    r"(?:[\ufe0f\u200d\U0001f3fb-\U0001f3ff]+"
    r"[\U0001f300-\U0001f9ff\u2600-\u26ff\u2700-\u27bf])*"
    r")"
)

# --- 1. Configuration & Scenarios ---

TARGET_FUNCS = [
    # Time (Polars)
    ("time_emoji_extractor", lambda lf: ee_time(lf, TIME_REGEX)),
    ("time_emoji_counter", ec_time),
    ("time_get_top_k", lambda lf: gk_time(lf, 2)),
    ("time_q2", q2_time),
    # Memory (Functional)
    ("mem_emoji_extractor", lambda fp: ee_mem(fp, MEM_REGEX)),
    ("mem_emoji_counter", ec_mem),
    ("mem_get_top_k", lambda c: gk_mem(c, 2)),
    ("mem_q2", q2_memory),
]

TEST_SCENARIOS = {
    "happy_path": {
        "data": [
            {"content": "I love ✈️ and ❤️"},
            {"content": "More ❤️"},
        ],
        "validators": {
            "time_emoji_extractor": lambda res: res.collect().height == 3,
            "time_emoji_counter": lambda res: res.collect().height == 2,
            "time_get_top_k": lambda res: res.collect().height == 2,
            "time_q2": lambda res: res[0][1] == 2,
            "mem_emoji_extractor": lambda res: len(list(res)) == 2,
            "mem_emoji_counter": lambda res: len(res) == 2,
            "mem_get_top_k": lambda res: len(res) == 2,
            "mem_q2": lambda res: res[0][1] == 2,
        },
    },
    "zwj_sequences": {
        "data": [
            {"content": "Family: 👨‍👩‍👧‍👦"},
        ],
        "validators": {
            "time_emoji_extractor": lambda res: res.collect().height >= 1,
            "time_emoji_counter": lambda res: res.collect().height >= 1,
            "time_get_top_k": lambda res: res.collect().height >= 1,
            "time_q2": lambda res: res[0][1] == 1,
            "mem_emoji_extractor": lambda res: len(list(res)[0]) == 1,
            "mem_emoji_counter": lambda res: len(res) == 1,
            "mem_get_top_k": lambda res: len(res) == 1,
            "mem_q2": lambda res: res[0][0] == "👨‍👩‍👧‍👦",
        },
    },
    "tie_breaking": {
        "data": [
            {"content": "😊 ✈️"},
        ],
        "validators": {
            "time_emoji_extractor": lambda res: res.collect().height == 2,
            "time_emoji_counter": lambda res: res.collect().height == 2,
            "time_get_top_k": lambda res: res.collect().height == 2,
            "time_q2": lambda res: res[0][1] == res[1][1] and res[0][0] < res[1][0],
            "mem_emoji_extractor": lambda res: len(list(res)[0]) == 2,
            "mem_emoji_counter": lambda res: len(res) == 2,
            "mem_get_top_k": lambda res: len(res) == 2,
            "mem_q2": lambda res: res[0][1] == res[1][1] and res[0][0] < res[1][0],
        },
    },
    "empty": {
        "data": [],
        "validators": {
            "time_emoji_extractor": lambda res: res.collect().height == 0,
            "time_emoji_counter": lambda res: res.collect().height == 0,
            "time_get_top_k": lambda res: res.collect().height == 0,
            "time_q2": lambda res: res == [],
            "mem_emoji_extractor": lambda res: list(res) == [],
            "mem_emoji_counter": lambda res: len(res) == 0,
            "mem_get_top_k": lambda res: len(res) == 0,
            "mem_q2": lambda res: res == [],
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
def test_q2_engine(json_factory, func_name, func_impl, scenario_name):
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
            input_data = file_path  # Memory extractor takes file_path
        result = func_impl(input_data)

    elif "counter" in func_name:
        if "time" in func_name:
            from src.common.utils import read_polars

            lf = read_polars(file_path)
            input_data = ee_time(lf, TIME_REGEX)
        else:
            input_data = ee_mem(file_path, MEM_REGEX)
        result = func_impl(input_data)

    elif "get_top_k" in func_name:
        if "time" in func_name:
            from src.common.utils import read_polars

            lf = read_polars(file_path)
            counts_lf = ec_time(ee_time(lf, TIME_REGEX))
            result = func_impl(counts_lf)
        else:
            counter = ec_mem(ee_mem(file_path, MEM_REGEX))
            result = func_impl(counter)

    else:  # q2_time, q2_memory
        result = func_impl(file_path)

    assert validator(result)
