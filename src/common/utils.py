import msgspec
import polars as pl
from collections.abc import Iterable
import io


# --- 1. ESQUEMA EXPLÍCITO (Optimización Polars) ---

twitter_schema = {
    "id": pl.Int64,
    "date": pl.String,
    "content": pl.String,
    "user": pl.Struct([pl.Field("id", pl.Int64), pl.Field("username", pl.String)]),
    "mentionedUsers": pl.List(pl.Struct([pl.Field("username", pl.String)])),
}


def read_polars(file_path: str):
    """
    Lee archivos NDJSON usando Polars Lazy. Soporta local y GCS (gs://).
    """
    return pl.scan_ndjson(file_path, schema=twitter_schema, ignore_errors=True)


# --- 2. MSGSPEC STRUCTS (Optimización Memoria/Streaming) ---


class User(msgspec.Struct):
    username: str


class Tweet(msgspec.Struct):
    date: str
    user: User


class ContentTweet(msgspec.Struct):
    content: str


class Mention(msgspec.Struct):
    username: str


class MentionTweet(msgspec.Struct):
    mentionedUsers: list[Mention] | None


# Decodificadores pre-compilados (Performance)
tweet_decoder = msgspec.json.Decoder(Tweet)
content_decoder = msgspec.json.Decoder(ContentTweet)
mention_decoder = msgspec.json.Decoder(MentionTweet)


def _get_gcs_blob(file_path: str):
    """Auxiliar para obtener el blob de GCS."""
    from google.cloud import storage

    path_parts = file_path.replace("gs://", "").split("/")
    bucket_name = path_parts[0]
    blob_name = "/".join(path_parts[1:])
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_name)


def read_orjson(file_path: str) -> Iterable[dict]:
    """
    Mantiene compatibilidad con código existente que usa orjson,
    pero utiliza msgspec internamente para mayor velocidad si es posible.
    """
    yield from read_msgspec(file_path, decoder=None)


def read_msgspec(file_path: str, decoder=None) -> Iterable:
    """
    Generador que lee archivos JSONL línea a línea usando msgspec para validación ultra-rápida.
    Si decoder es None, usa msgspec para cargar un dict genérico.
    """
    if decoder is None:
        # Cargador de dict genérico ultra-rápido
        decoder = msgspec.json.Decoder()

    if file_path.startswith("gs://"):
        blob = _get_gcs_blob(file_path)
        stream = io.BytesIO()
        blob.download_to_file(stream)
        stream.seek(0)
        file_obj = stream
    else:
        file_obj = open(file_path, "rb")

    try:
        for line in file_obj:
            try:
                yield decoder.decode(line)
            except msgspec.DecodeError:
                continue
    finally:
        file_obj.close()
