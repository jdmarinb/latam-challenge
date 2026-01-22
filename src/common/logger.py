import time
import functools
import traceback
import structlog
import orjson
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


class WideEventContext:
    """Accumulates context and metrics for a Wide Event."""

    def __init__(self):
        self.steps = {}
        self.metrics = {}
        self.extra_context = {}

    def add_step(self, name: str, duration_ms: float, **metadata):
        self.steps[name] = {"duration_ms": duration_ms, **metadata}

    def add_metric(self, name: str, value: Any):
        self.metrics[name] = value

    def add_context(self, **kwargs):
        self.extra_context.update(kwargs)


def canonical_logger(event_name: str):
    """
    Decorator for Canonical Logging (Wide Events) using structlog.
    Injects a 'ctx' object into the function to accumulate metadata.
    Emits ONE single structured JSON log event upon completion.
    """

    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            ctx = WideEventContext()
            start_time = time.perf_counter()
            status = "success"
            error_reason = None
            stack_trace = None
            result = None

            # Inject ctx into kwargs if the function accepts it
            # Using a slightly safer approach to check for 'ctx' in signature
            # but for simplicity in this minimalist environment, we'll just pass it.
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
                total_duration_ms = round((end_time - start_time) * 1000, 2)

                log_data = {
                    "event": event_name,
                    "status": status,
                    "total_duration_ms": total_duration_ms,
                    "context": {
                        "function": func.__name__,
                        **ctx.extra_context,
                    },
                    "metrics": ctx.metrics,
                    "steps": ctx.steps,
                }

                if status == "failure":
                    log_data["failure_reason"] = error_reason
                    log_data["stack_trace"] = stack_trace

                logger.info(**log_data)

        return wrapper

    return decorator
