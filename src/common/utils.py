import orjson
import polars as pl
from collections.abc import Iterable


# --- 1. ESQUEMA EXPLÍCITO (Optimización Polars) ---

twitter_schema = {
    "id": pl.Int64,  # Added for error reporting
    "date": pl.String,
    "content": pl.String,
    "user": pl.Struct([pl.Field("id", pl.Int64), pl.Field("username", pl.String)]),
    "mentionedUsers": pl.List(pl.Struct([pl.Field("username", pl.String)])),
}


def read_polars(file_path):
    # Strategy 1: ignore_errors=True prevents crash on corrupt lines
    return pl.scan_ndjson(file_path, schema=twitter_schema, ignore_errors=True)


def read_orjson(file_path: str) -> Iterable[dict]:
    with open(file_path, "rb") as f:
        for line in f:
            try:
                yield orjson.loads(line)
            except orjson.JSONDecodeError:
                # Strategy 1: Log or yield special error record could go here,
                # but following the principle of minimal intervention for now.
                continue
