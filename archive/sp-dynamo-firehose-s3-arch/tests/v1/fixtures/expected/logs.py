"""JSON-structured logging shared by the pipeline's jobs."""

import json
import logging
import sys
from datetime import UTC, datetime

STANDARD_LOG_RECORD_FIELDS = frozenset(vars(logging.makeLogRecord({}))) | {"message"}


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        """Formats a log record as JSON.

        Args:
            record: The log record to format.

        Returns:
            The record serialized as a JSON string. Values passed through the
            logging call's `extra` argument appear as top-level fields.
        """
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload |= {
            key: value
            for key, value in vars(record).items()
            if key not in STANDARD_LOG_RECORD_FIELDS
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging() -> None:
    """Sends JSON-structured logs to stdout and reduces py4j log noise."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    logging.getLogger("py4j").setLevel(logging.WARNING)
