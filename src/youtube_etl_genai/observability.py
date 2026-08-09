"""Structured operational telemetry for Databricks Job tasks."""

from __future__ import annotations

import json
import logging
import sys
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any

from youtube_etl_genai.persistence import merge_task_execution_log


class _JsonFormatter(logging.Formatter):
    """Render the explicit event payload as one JSON document per log line."""

    def format(self, record: logging.LogRecord) -> str:
        payload = dict(getattr(record, "event_payload", {}))
        payload.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        payload.setdefault("level", record.levelname)
        payload.setdefault("logger", record.name)
        if record.exc_info:
            payload["stacktrace"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, sort_keys=True)


def configure_job_logging() -> logging.Logger:
    """Configure the package logger once, without changing Databricks root logging."""
    logger = logging.getLogger("youtube_etl_genai")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not any(getattr(handler, "_youtube_json", False) for handler in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler._youtube_json = True  # type: ignore[attr-defined]
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    return logger


def log_event(
    logger: logging.Logger, event: str, *, level: int = logging.INFO, **context: Any
) -> None:
    """Emit one structured, secret-free event through the configured logger."""
    logger.log(level, event, extra={"event_payload": {"event": event, **context}})


class TaskExecution(AbstractContextManager["TaskExecution"]):
    """Log and persist one task attempt with an explicit business outcome."""

    def __init__(
        self,
        *,
        spark: Any,
        catalog: str,
        task_key: str,
        task_run_id: str | None,
        ingestion_id: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.task_key = task_key
        self.task_run_id = task_run_id
        self.ingestion_id = ingestion_id
        self.logger = logger or logging.getLogger(__name__)
        self.started_at = datetime.now(timezone.utc)
        self.status = "FAILED"
        self.counts: dict[str, int] = {}
        self.api_cost_units = 0
        self.error_message: str | None = None

    def __enter__(self) -> "TaskExecution":
        log_event(
            self.logger,
            "step_start",
            ingestion_id=self.ingestion_id,
            task_key=self.task_key,
            task_run_id=self.task_run_id,
        )
        return self

    def complete(
        self,
        *,
        status: str,
        counts: dict[str, int] | None = None,
        api_cost_units: int = 0,
    ) -> None:
        """Set the semantic result before the context emits its end event."""
        self.status = status
        self.counts = counts or {}
        self.api_cost_units = max(self.api_cost_units, api_cost_units)

    def add_api_cost(self, cost_units: int) -> None:
        """Accumulate a request cost even if the task later raises an error."""
        self.api_cost_units += cost_units

    def complete_from_result(self, result: dict[str, int | str]) -> None:
        """Copy the uniform pipeline result into this task execution summary."""
        if result.get("ingestion_id"):
            self.ingestion_id = str(result["ingestion_id"])
        self.complete(
            status=str(result["status"]),
            counts={
                key: int(result.get(key, 0))
                for key in (
                    "videos_attempted",
                    "videos_succeeded",
                    "videos_failed",
                    "records_fetched",
                )
            },
            api_cost_units=int(result.get("api_cost_units", 0)),
        )

    def _persist(self, ended_at: datetime) -> None:
        merge_task_execution_log(
            self.spark,
            self.catalog,
            ingestion_id=self.ingestion_id,
            task_key=self.task_key,
            task_run_id=self.task_run_id,
            started_at=self.started_at,
            ended_at=ended_at,
            status=self.status,
            videos_attempted=self.counts.get("videos_attempted", 0),
            videos_succeeded=self.counts.get("videos_succeeded", 0),
            videos_failed=self.counts.get("videos_failed", 0),
            records_fetched=self.counts.get("records_fetched", 0),
            api_cost_units=self.api_cost_units,
            error_message=self.error_message,
        )

    def __exit__(self, exc_type: Any, exc: BaseException | None, traceback: Any) -> bool:
        ended_at = datetime.now(timezone.utc)
        duration_seconds = (ended_at - self.started_at).total_seconds()
        if exc is not None:
            self.status = "FAILED"
            self.error_message = str(exc)
            self.logger.exception(
                "step_failure",
                extra={
                    "event_payload": {
                        "event": "step_failure",
                        "ingestion_id": self.ingestion_id,
                        "task_key": self.task_key,
                        "task_run_id": self.task_run_id,
                        "duration_seconds": duration_seconds,
                        "error_type": type(exc).__name__,
                        "error": self.error_message,
                    }
                },
            )
        else:
            log_event(
                self.logger,
                "step_end",
                ingestion_id=self.ingestion_id,
                task_key=self.task_key,
                task_run_id=self.task_run_id,
                status=self.status,
                duration_seconds=duration_seconds,
                api_cost_units=self.api_cost_units,
                **self.counts,
            )
        try:
            self._persist(ended_at)
        except Exception:
            self.logger.exception(
                "task_execution_log_persistence_failure",
                extra={
                    "event_payload": {
                        "event": "task_execution_log_persistence_failure",
                        "ingestion_id": self.ingestion_id,
                        "task_key": self.task_key,
                        "task_run_id": self.task_run_id,
                    }
                },
            )
            if exc is None:
                raise
        return False
