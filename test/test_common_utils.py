import pytest
import polars as pl
import json
from src.common.utils import read_polars, read_orjson

# --- 1. Configuration & Scenarios ---

TARGET_FUNCS = [
    ("polars", read_polars),
    ("orjson", read_orjson),
]

TEST_SCENARIOS = {
    "happy_path": {
        "data": [
            {"date": "2021-02-12", "content": "A", "user": {"username": "u1"}},
            {"date": "2021-02-13", "content": "B", "user": {"username": "u2"}},
        ],
        "validators": {
            "polars": lambda res: res.collect().height == 2
            and res.collect_schema()["date"] == pl.String,
            "orjson": lambda res: len(list(res)) == 2,
        },
    },
    "corrupt_lines": {
        "raw_content": '{"date": "A"}\n{"date": "BROKEN \n{"date": "B"}\n',
        "validators": {
            "polars": lambda res: True,  # Logic moved to try-except in driver
            "orjson": lambda res: len(list(res)) == 2,
        },
    },
    "empty_file": {
        "data": [],
        "raw_content": "",
        "validators": {
            "polars": lambda res: res.collect().height == 0,
            "orjson": lambda res: list(res) == [],
        },
    },
    "null_values": {
        "data": [{"date": "2021-02-12", "content": None, "user": None}],
        "validators": {
            "polars": lambda res: res.collect()["user"][0] is None,
            "orjson": lambda res: list(res)[0]["user"] is None,
        },
    },
}

# --- 2. Shared Fixtures ---


@pytest.fixture
def json_factory(tmp_path):
    def _create(filename, content):
        p = tmp_path / filename
        if isinstance(content, list):
            with open(p, "w") as f:
                for item in content:
                    f.write(json.dumps(item) + "\n")
        else:
            p.write_text(content or "")
        return str(p)

    return _create


# --- 3. The Driver Test Function ---


@pytest.mark.parametrize("func_name, func_impl", TARGET_FUNCS)
@pytest.mark.parametrize("scenario_name", TEST_SCENARIOS.keys())
def test_engine_processing(json_factory, func_name, func_impl, scenario_name):
    config = TEST_SCENARIOS[scenario_name]
    input_data = config.get("data") if "data" in config else config.get("raw_content")
    file_path = json_factory(f"{scenario_name}.json", input_data)

    try:
        result = func_impl(file_path)

        if func_name == "polars" and scenario_name == "corrupt_lines":
            with pytest.raises(pl.exceptions.ComputeError):
                result.collect()
            return

    except pl.exceptions.ComputeError:
        if func_name == "polars" and scenario_name == "corrupt_lines":
            return
        raise
    except Exception as e:
        pytest.fail(f"Execution failed for {func_name} in {scenario_name}: {e}")

    validator = config["validators"].get(func_name)
    if validator:
        assert validator(result), (
            f"Validator failed for {func_name} in case {scenario_name}"
        )
