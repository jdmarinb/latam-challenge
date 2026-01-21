import orjson
import polars as pl
from collections.abc import Iterable


# --- 1. ESQUEMA EXPLÍCITO (Optimización Polars) ---

twitter_schema = {
    "date": pl.String,
    "content": pl.String,
    "user": pl.Struct([pl.Field("id", pl.Int64), pl.Field("username", pl.String)]),
    "mentionedUsers": pl.List(pl.Struct([pl.Field("username", pl.String)])),
}


def read_polars(file_path):
    return pl.scan_ndjson(file_path, schema=twitter_schema)


def read_orjson(file_path: str) -> Iterable[dict]:
    with open(file_path, "rb") as f:
        for line in f:
            try:
                yield orjson.loads(line)
            except orjson.JSONDecodeError:
                continue
