import json
import logging
import sys
import time
from common.context import get_correlation_id


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "correlation_id": getattr(record, "correlation_id", None) or get_correlation_id(),
        }

        if record.exc_info:
            log["exception"] = self.formatException(record.exc_info)

        record_data = record.__dict__.copy()
        for remove_key in [
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "asctime",
        ]:
            record_data.pop(remove_key, None)

        if record_data:
            log.update(record_data)

        return json.dumps(log)


class Span:
    def __init__(self, name, logger):
        self.name = name
        self.logger = logger
        self.start = None

    def __enter__(self):
        self.start = time.perf_counter()
        self.logger.info(
            f"span.start {self.name}",
            extra={"span": self.name, "span_event": "start"},
        )
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        duration = round(time.perf_counter() - self.start, 6)
        if exc_type:
            self.logger.exception(
                f"span.error {self.name}",
                extra={"span": self.name, "span_event": "error", "duration": duration},
            )
            return False

        self.logger.info(
            f"span.end {self.name}",
            extra={"span": self.name, "span_event": "end", "duration": duration},
        )


def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger