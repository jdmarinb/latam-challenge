import polars as pl
import orjson
from collections.abc import Iterable


# --- 1. ESQUEMA EXPLÍCITO (Optimización Polars) ---

twitter_schema = {
    "date": pl.String,
    "content": pl.String,
    "user": pl.Struct([pl.Field("id", pl.Int64), pl.Field("username", pl.String)]),
    "mentionedUsers": pl.List(pl.Struct([pl.Field("username", pl.String)])),
}


def read_chunks_orjson(file_path: str, chunk_size: int = 5000) -> Iterable[list[dict]]:
    """Lee y entrega bloques de 5000 registros sin funciones extra."""
    with open(file_path, "rb") as f:
        chunk = []
        for line in f:
            try:
                chunk.append(orjson.loads(line))
                if len(chunk) == chunk_size:
                    yield chunk
                    chunk = []
            except orjson.JSONDecodeError:
                continue
        if chunk:
            yield chunk


def read_streaming_orjson(file_path: str) -> Iterable[dict]:
    with open(file_path, "rb") as f:
        for line in f:
            try:
                yield orjson.loads(line)
            except orjson.JSONDecodeError:
                continue
