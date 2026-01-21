import os
import re
import io
import sys
import time
import emoji
import pstats
import orjson
import cProfile
import functools
import unicodedata
import polars as pl
from collections import Counter
from typing import Callable, Any
from collections.abc import Iterable
from memory_profiler import memory_usage
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

file_path = "farmers-protest-tweets-2021-2-4.json"

twitter_schema = {
    "date": pl.String,
    "content": pl.String,
    "user": pl.Struct([pl.Field("id", pl.Int64), pl.Field("username", pl.String)]),
    "mentionedUsers": pl.List(pl.Struct([pl.Field("username", pl.String)])),
}


def measure_time(func: Callable, *args, **kwargs) -> tuple[float, Any]:
    """Responsabilidad: Medir el tiempo de ejecución y retornar resultado."""
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    return end_time - start_time, result


def measure_memory(func: Callable, *args, **kwargs) -> tuple[float, Any]:
    """Responsabilidad: Medir el pico de memoria y retornar resultado."""
    # Nota: memory_usage ejecuta la función internamente.
    # Para medir tiempo Y memoria en una sola ejecución reutilizando lógica,
    # el decorador debe orquestar.
    mem_samples, result = memory_usage((func, args, kwargs), interval=0.1, retval=True)
    return max(mem_samples), result


def profile_performance(func):
    """
    Decorador que orquesta la medición de rendimiento.
    Para no ejecutar la función dos veces, usamos measure_memory
    pasándole una versión de la función que ya mide su tiempo.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Creamos una función interna que usa measure_time para ser monitoreada por measure_memory
        def time_wrapped_func():
            return measure_time(func, *args, **kwargs)

        # Ejecutamos la medición de memoria sobre la función que mide tiempo
        peak_mem, (duration, result) = measure_memory(time_wrapped_func)

        print(f"\n[PERF] {func.__name__}:")
        print(f"  > Tiempo: {duration:.4f} s")
        print(f"  > Memoria: {peak_mem:.2f} MB")

        return result

    return wrapper


def profile_detailed(func):
    """
    Decorador independiente para análisis detallado con cProfile.
    Útil para detectar cuellos de botella, pero añade overhead al tiempo medido.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print(f"\n--- INICIANDO PERFILADO DETALLADO: {func.__name__} ---")
        profiler = cProfile.Profile()
        profiler.enable()
        result = func(*args, **kwargs)
        profiler.disable()

        s = io.StringIO()
        ps = pstats.Stats(profiler, stream=s).sort_stats("cumulative")
        ps.print_stats(15)  # Top 15 funciones
        print(s.getvalue())
        print(f"--- FIN PERFILADO DETALLADO: {func.__name__} ---\n")

        return result

    return wrapper


def read_streaming_orjson(file_path: str) -> Iterable[dict]:
    with open(file_path, "rb") as f:
        for line in f:
            try:
                yield orjson.loads(line)
            except orjson.JSONDecodeError:
                continue


def text_chunk_reader(file_path: str, chunk_size: int = 5000) -> Iterable[list[bytes]]:
    with open(file_path, "rb") as f:
        chunk = []
        for line in f:
            chunk.append(line)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


@profile_performance
def lab_modular_test(
    name: str, reader_func: Callable, processor_func: Callable, path: str
):
    print(f"\n[LAB] Escenario: {name}")
    data = reader_func(path)
    return processor_func(data)


@profile_performance
def lab_parallel_test(
    name: str, file_path: str, mode: str = "process", worker: Callable = None
):
    print(f"\n[LAB] Escenario: {name} ({mode.upper()})")
    total_counts = Counter()
    Executor = ProcessPoolExecutor if mode == "process" else ThreadPoolExecutor
    with Executor() as executor:
        results = executor.map(worker, text_chunk_reader(file_path))
        for local_counts in results:
            total_counts.update(local_counts)
    return len(total_counts)


# --- 3. CAPA DE PROCESAMIENTO (Processor Layer) ---

# --- Q1: Usuarios ---


def process_q1_functional(tweets: Iterable[dict]) -> int:
    def extract_and_clean(t):
        user = t.get("user", {}) if isinstance(t, dict) else {}
        u_name = user.get("username")
        if u_name:
            return sys.intern(unicodedata.normalize("NFKC", u_name).lower())
        return None

    cleaned_names = filter(None, map(extract_and_clean, tweets))
    return len(Counter(cleaned_names))


def process_q1_parallel_worker(chunk: list[bytes]) -> Counter:
    local_counts = Counter()
    for line in chunk:
        try:
            tweet = orjson.loads(line)
            u_name = tweet.get("user", {}).get("username")
            if u_name:
                local_counts[
                    sys.intern(unicodedata.normalize("NFKC", u_name).lower())
                ] += 1
        except orjson.JSONDecodeError:
            continue
    return local_counts


# --- Q2: Emojis ---


def process_q2_functional(tweets: Iterable[dict]) -> int:
    """Extrae emojis usando la librería emoji para soportar ZWJ."""
    emoji_counts = Counter()
    for t in tweets:
        content = t.get("content")
        if content:
            # emoji.analyze captura secuencias ZWJ como una sola unidad
            emojis = [e.chars for e in emoji.analyze(content)]
            emoji_counts.update(emojis)
    return len(emoji_counts)


def process_q2_parallel_worker(chunk: list[bytes]) -> Counter:
    local_counts = Counter()
    for line in chunk:
        try:
            tweet = orjson.loads(line)
            content = tweet.get("content")
            if content:
                emojis = [e.chars for e in emoji.analyze(content)]
                local_counts.update(emojis)
        except orjson.JSONDecodeError:
            continue
    return local_counts


# --- Q2: Emojis (Versión REGEX para Benchmark) ---


def process_q2_regex_functional(tweets: Iterable[dict]) -> int:
    """Extrae emojis usando Regex simple (Rápido pero menos preciso). Dos."""
    # Regex para Python (Soporta escapes \u)
    emoji_pattern = re.compile(
        r"[\u2600-\u27BF]|[\U0001F300-\U0001F6FF]|[\U0001F900-\U0001F9FF]"
    )
    emoji_counts = Counter()
    for t in tweets:
        content = t.get("content")
        if content:
            emojis = emoji_pattern.findall(content)
            emoji_counts.update(emojis)
    return len(emoji_counts)


def process_q2_regex_parallel_worker(chunk: list[bytes]) -> Counter:
    local_counts = Counter()
    emoji_pattern = re.compile(r"[\u2600-\u27BF]|[\uD83C-\uD83E][\uDC00-\uDFFF]")
    for line in chunk:
        try:
            tweet = orjson.loads(line)
            content = tweet.get("content")
            if content:
                emojis = emoji_pattern.findall(content)
                local_counts.update(emojis)
        except orjson.JSONDecodeError:
            continue
    return local_counts


# --- Q2: Emojis (Versión ROBUST REGEX para Benchmark) ---

# Patrón Python compatible con los escalares usados en Polars
ROBUST_EMOJI_PYTHON = re.compile(
    r"("
    r"(?:[\U0001F3FB-\U0001F3FF])|"
    r"(?:[\u2600-\u27BF])|"
    r"(?:[\U0001F300-\U0001FAFF])|"
    r"(?:[\u200D])"
    r")+"
)


def process_q2_robust_regex_functional(tweets: Iterable[dict]) -> int:
    """Extrae emojis usando Robust Regex (Soporte ZWJ)."""
    emoji_counts = Counter()
    for t in tweets:
        content = t.get("content")
        if content:
            emojis = ROBUST_EMOJI_PYTHON.findall(content)
            emoji_counts.update(emojis)
    return len(emoji_counts)


def process_q2_robust_regex_parallel_worker(chunk: list[bytes]) -> Counter:
    local_counts = Counter()
    for line in chunk:
        try:
            tweet = orjson.loads(line)
            content = tweet.get("content")
            if content:
                emojis = ROBUST_EMOJI_PYTHON.findall(content)
                local_counts.update(emojis)
        except orjson.JSONDecodeError:
            continue
    return local_counts


# --- Q3: Menciones ---


def process_q3_functional(tweets: Iterable[dict]) -> int:
    """Extrae menciones usando metadatos (mentionedUsers) y normalización."""
    mention_counts = Counter()
    for t in tweets:
        mentions = t.get("mentionedUsers")
        if mentions:
            extracted = [
                sys.intern(unicodedata.normalize("NFKC", m.get("username")).lower())
                for m in mentions
                if m.get("username")
            ]
            mention_counts.update(extracted)
    return len(mention_counts)


def process_q3_parallel_worker(chunk: list[bytes]) -> Counter:
    local_counts = Counter()
    for line in chunk:
        try:
            tweet = orjson.loads(line)
            mentions = tweet.get("mentionedUsers")
            if mentions:
                for m in mentions:
                    u_name = m.get("username")
                    if u_name:
                        norm_name = sys.intern(
                            unicodedata.normalize("NFKC", u_name).lower()
                        )
                        local_counts[norm_name] += 1
        except orjson.JSONDecodeError:
            continue
    return local_counts


# --- 4. ORQUESTADORES DE LABORATORIO ---


def run_lab():
    print(
        "\n-------------------------------------------------------------------------\n"
    )
    print("Comparando todas las Combinaciones pregunta 1")

    lab_modular_test(
        "ORJSON STREAMING", read_streaming_orjson, process_q1_functional, file_path
    )
    lab_parallel_test(
        "ORJSON + MULTITHREADING",
        file_path,
        mode="thread",
        worker=process_q1_parallel_worker,
    )
    lab_parallel_test(
        "ORJSON + MULTIPROCESSING",
        file_path,
        mode="process",
        worker=process_q1_parallel_worker,
    )

    lab_modular_test(
        "POLARS LAZY",
        lambda f: pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: lf.select(
            pl.col("user").struct.field("username").str.to_lowercase().alias("username")
        )
        .filter(pl.col("username").is_not_null())
        .group_by("username")
        .len()
        .collect(),
        file_path,
    )

    lab_modular_test(
        "POLARS STREAMING",
        lambda f: pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: (
            lf.select(
                pl.col("user")
                .struct.field("username")
                .str.to_lowercase()
                .alias("username")
            )
            .filter(pl.col("username").is_not_null())
            .group_by("username")
            .len()
            .collect(engine="streaming")
        ),
        file_path,
    )

    lab_modular_test(
        "POLARS EAGER",
        lambda f: pl.read_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda df: df.select(
            pl.col("user").struct.field("username").str.to_lowercase().alias("username")
        )
        .filter(pl.col("username").is_not_null())
        .group_by("username")
        .len(),
        file_path,
    )
    print(
        "\n-------------------------------------------------------------------------\n"
    )
    print("Comparando todas las Combinaciones pregunta 2 usando emoji")

    lab_modular_test(
        "ORJSON STREAMING", read_streaming_orjson, process_q2_functional, file_path
    )
    lab_parallel_test(
        "ORJSON + MULTITHREADING",
        file_path,
        mode="thread",
        worker=process_q2_parallel_worker,
    )
    lab_parallel_test(
        "ORJSON + MULTIPROCESSING",
        file_path,
        mode="process",
        worker=process_q2_parallel_worker,
    )

    # Polars Q2: Extracción de emojis usando map_elements con la librería emoji (Sin Regex)
    def extract_emojis_map(content):
        if content:
            return [e.chars for e in emoji.analyze(content)]
        return []

    lab_modular_test(
        "POLARS LAZY",
        lambda f: pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: lf.select(
            pl.col("content")
            .map_elements(extract_emojis_map, return_dtype=pl.List(pl.String))
            .alias("emojis")
        )
        .explode("emojis")
        .filter(pl.col("emojis").is_not_null())
        .group_by("emojis")
        .len()
        .collect(),
        file_path,
    )

    lab_modular_test(
        "POLARS STREAMING",
        lambda f: pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: lf.select(
            pl.col("content")
            .map_elements(extract_emojis_map, return_dtype=pl.List(pl.String))
            .alias("emojis")
        )
        .explode("emojis")
        .filter(pl.col("emojis").is_not_null())
        .group_by("emojis")
        .len()
        .collect(engine="streaming"),
        file_path,
    )

    lab_modular_test(
        "POLARS EAGER",
        lambda f: pl.read_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda df: df.select(
            pl.col("content")
            .map_elements(extract_emojis_map, return_dtype=pl.List(pl.String))
            .alias("emojis")
        )
        .explode("emojis")
        .filter(pl.col("emojis").is_not_null())
        .group_by("emojis")
        .len(),
        file_path,
    )

    print(
        "\n-------------------------------------------------------------------------\n"
    )
    print("Comparando todas las Combinaciones pregunta 2 usando regex")

    # Regex compatible con Rust/Polars (Soporta escalares directos)
    polars_emoji_regex = (
        r"[\u2600-\u27BF]|[\U0001F300-\U0001F6FF]|[\U0001F900-\U0001F9FF]"
    )

    lab_modular_test(
        "ORJSON STREAMING (REGEX)",
        read_streaming_orjson,
        process_q2_regex_functional,
        file_path,
    )
    lab_parallel_test(
        "ORJSON + MULTITHREADING (REGEX)",
        file_path,
        mode="thread",
        worker=process_q2_regex_parallel_worker,
    )
    lab_parallel_test(
        "ORJSON + MULTIPROCESSING (REGEX)",
        file_path,
        mode="process",
        worker=process_q2_regex_parallel_worker,
    )

    lab_modular_test(
        "POLARS LAZY (REGEX)",
        lambda f: pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: lf.select(
            pl.col("content").str.extract_all(polars_emoji_regex).alias("emojis")
        )
        .explode("emojis")
        .filter(pl.col("emojis").is_not_null())
        .group_by("emojis")
        .len()
        .collect(),
        file_path,
    )

    lab_modular_test(
        "POLARS STREAMING (REGEX)",
        lambda f: pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: lf.select(
            pl.col("content").str.extract_all(polars_emoji_regex).alias("emojis")
        )
        .explode("emojis")
        .filter(pl.col("emojis").is_not_null())
        .group_by("emojis")
        .len()
        .collect(engine="streaming"),
        file_path,
    )

    lab_modular_test(
        "POLARS EAGER (REGEX)",
        lambda f: pl.read_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda df: df.select(
            pl.col("content").str.extract_all(polars_emoji_regex).alias("emojis")
        )
        .explode("emojis")
        .filter(pl.col("emojis").is_not_null())
        .group_by("emojis")
        .len(),
        file_path,
    )

    print(
        "\n-------------------------------------------------------------------------\n"
    )
    print(
        "Comparando todas las Combinaciones pregunta 2 usando robust regex (ZWJ Support)"
    )

    lab_modular_test(
        "ORJSON STREAMING (ROBUST REGEX)",
        read_streaming_orjson,
        process_q2_robust_regex_functional,
        file_path,
    )
    lab_parallel_test(
        "ORJSON + MULTITHREADING (ROBUST REGEX)",
        file_path,
        mode="thread",
        worker=process_q2_robust_regex_parallel_worker,
    )
    lab_parallel_test(
        "ORJSON + MULTIPROCESSING (ROBUST REGEX)",
        file_path,
        mode="process",
        worker=process_q2_robust_regex_parallel_worker,
    )

    # Regex Robusta compatible con Rust/Polars
    robust_emoji_rust = (
        r"("
        r"(?:[\U0001F3FB-\U0001F3FF])|"  # Skin tones
        r"(?:[\u2600-\u27BF])|"  # Basic blocks
        r"(?:[\U0001F300-\U0001FAFF])|"  # Extended blocks
        r"(?:[\u200D])"  # Zero Width Joiner
        r")+"
    )

    lab_modular_test(
        "POLARS LAZY (ROBUST REGEX)",
        lambda f: pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: lf.select(
            pl.col("content").str.extract_all(robust_emoji_rust).alias("emojis")
        )
        .explode("emojis")
        .filter(pl.col("emojis").is_not_null())
        .group_by("emojis")
        .len()
        .collect(),
        file_path,
    )

    lab_modular_test(
        "POLARS STREAMING (ROBUST REGEX)",
        lambda f: pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: lf.select(
            pl.col("content").str.extract_all(robust_emoji_rust).alias("emojis")
        )
        .explode("emojis")
        .filter(pl.col("emojis").is_not_null())
        .group_by("emojis")
        .len()
        .collect(engine="streaming"),
        file_path,
    )

    print(
        "\n-------------------------------------------------------------------------\n"
    )
    print("Comparando todas las Combinaciones pregunta 3")

    lab_modular_test(
        "ORJSON STREAMING", read_streaming_orjson, process_q3_functional, file_path
    )
    lab_parallel_test(
        "ORJSON + MULTITHREADING",
        file_path,
        mode="thread",
        worker=process_q3_parallel_worker,
    )
    lab_parallel_test(
        "ORJSON + MULTIPROCESSING",
        file_path,
        mode="process",
        worker=process_q3_parallel_worker,
    )

    lab_modular_test(
        "POLARS LAZY",
        lambda f: pl.scan_ndjson(
            pl.read_ndjson(f, schema=twitter_schema, ignore_errors=True)
            .to_pandas()
            .to_json(orient="records", lines=True)
        ).scan_ndjson(f, schema=twitter_schema, ignore_errors=True)
        if False
        else pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: lf.explode("mentionedUsers")
        .filter(pl.col("mentionedUsers").is_not_null())
        .select(
            pl.col("mentionedUsers")
            .struct.field("username")
            .str.to_lowercase()
            .alias("username")
        )
        .filter(pl.col("username").is_not_null())
        .group_by("username")
        .len()
        .collect(),
        file_path,
    )

    lab_modular_test(
        "POLARS STREAMING",
        lambda f: pl.scan_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda lf: lf.explode("mentionedUsers")
        .filter(pl.col("mentionedUsers").is_not_null())
        .select(
            pl.col("mentionedUsers")
            .struct.field("username")
            .str.to_lowercase()
            .alias("username")
        )
        .filter(pl.col("username").is_not_null())
        .group_by("username")
        .len()
        .collect(engine="streaming"),
        file_path,
    )

    lab_modular_test(
        "POLARS EAGER",
        lambda f: pl.read_ndjson(f, schema=twitter_schema, ignore_errors=True),
        lambda df: df.explode("mentionedUsers")
        .filter(pl.col("mentionedUsers").is_not_null())
        .select(
            pl.col("mentionedUsers")
            .struct.field("username")
            .str.to_lowercase()
            .alias("username")
        )
        .filter(pl.col("username").is_not_null())
        .group_by("username")
        .len(),
        file_path,
    )


if __name__ == "__main__":
    import os

    if os.path.exists(file_path):
        run_lab()
    else:
        print(f"Error: No se encuentra {file_path}")
