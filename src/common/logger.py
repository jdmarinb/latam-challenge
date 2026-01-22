import time
import functools
import traceback
import structlog
import orjson
import os
import psutil
from typing import Callable, Any


# Configure structlog for high performance with orjson
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(serializer=orjson.dumps),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()


def get_memory_usage_mb() -> float:
    """Returns the current process memory usage (RSS) in MB using psutil."""
    process = psutil.Process(os.getpid())
    return round(process.memory_info().rss / (1024 * 1024), 2)


class WideEventContext:
    """Accumulates context, metrics, and errors for a Wide Event."""

    def __init__(self):
        self.steps = {}
        self.metrics = {}
        self.extra_context = {}
        self.errors = []

    def add_step(self, name: str, duration_ms: float, **metadata):
        # Capture current memory at the end of the step
        current_mem = get_memory_usage_mb()
        self.steps[name] = {
            "duration_ms": duration_ms,
            "memory_mb": current_mem,
            **metadata,
        }

    def add_metric(self, name: str, value: Any):
        self.metrics[name] = value

    def add_context(self, **kwargs):
        self.extra_context.update(kwargs)

    def register_error(self, error_type: str, message: str, **details):
        """Registers a non-fatal error or edge case."""
        self.errors.append(
            {
                "type": error_type,
                "message": message,
                "timestamp": time.time(),
                "memory_mb": get_memory_usage_mb(),
                **details,
            }
        )


def canonical_logger(event_name: str):
    """
    Decorator for Canonical Logging (Wide Events) using structlog.
    Injects a 'ctx' object into the function to accumulate metadata.
    Emits ONE single structured JSON log event upon completion with time and memory.
    """

    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            ctx = WideEventContext()
            start_time = time.perf_counter()
            start_mem = get_memory_usage_mb()
            status = "success"
            error_reason = None
            stack_trace = None
            result = None

            try:
                result = func(*args, ctx=ctx, **kwargs)
                return result
            except Exception as e:
                status = "failure"
                error_reason = str(e)
                stack_trace = traceback.format_exc()
                raise e
            finally:
                end_time = time.perf_counter()
                end_mem = get_memory_usage_mb()
                total_duration_ms = round((end_time - start_time) * 1000, 2)

                log_data = {
                    "event": event_name,
                    "status": status,
                    "total_duration_ms": total_duration_ms,
                    "memory_usage": {
                        "start_mb": start_mem,
                        "end_mb": end_mem,
                        "delta_mb": round(end_mem - start_mem, 2),
                    },
                    "context": {
                        "function": func.__name__,
                        **ctx.extra_context,
                    },
                    "metrics": ctx.metrics,
                    "steps": ctx.steps,
                }

                if ctx.errors:
                    log_data["non_fatal_errors"] = ctx.errors

                if status == "failure":
                    log_data["failure_reason"] = error_reason
                    log_data["stack_trace"] = stack_trace
                    logger.error(**log_data)
                else:
                    logger.info(**log_data)

        return wrapper

    return decorator
