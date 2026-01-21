import time
import io
import cProfile
import pstats
import functools
from memory_profiler import memory_usage
from typing import Callable, Any


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
