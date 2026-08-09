from __future__ import annotations

from datetime import datetime, timezone

import pytest

from youtube_etl_genai.observability import TaskExecution
from youtube_etl_genai.persistence import merge_task_execution_log


class _FakeFrame:
    def __init__(self, spark: "_FakeSpark") -> None:
        self.spark = spark

    def createOrReplaceTempView(self, name: str) -> None:
        self.spark.view_name = name


class _FakeCatalog:
    def __init__(self) -> None:
        self.dropped_views: list[str] = []

    def dropTempView(self, name: str) -> None:
        self.dropped_views.append(name)


class _FakeSpark:
    def __init__(self) -> None:
        self.rows: list[tuple[object, ...]] = []
        self.schema = ""
        self.sql_text = ""
        self.view_name = ""
        self.catalog = _FakeCatalog()

    def createDataFrame(self, rows: list[tuple[object, ...]], schema: str) -> _FakeFrame:
        self.rows = rows
        self.schema = schema
        return _FakeFrame(self)

    def sql(self, statement: str) -> None:
        self.sql_text = statement


def test_merge_task_execution_log_uses_an_idempotent_merge() -> None:
    spark = _FakeSpark()
    now = datetime(2026, 8, 9, tzinfo=timezone.utc)

    merge_task_execution_log(
        spark,
        "catalog",
        ingestion_id="run-1",
        task_key="fetch_comments",
        task_run_id="task-run-1",
        started_at=now,
        ended_at=now,
        status="PARTIAL_SUCCESS",
        videos_attempted=2,
        videos_succeeded=1,
        videos_failed=1,
        records_fetched=10,
        api_cost_units=3,
        error_message=None,
    )

    assert "MERGE INTO catalog.control.task_execution_logs" in spark.sql_text
    assert "COALESCE(target.task_run_id, '')" in spark.sql_text
    assert spark.rows[0][0:3] == ("run-1", "fetch_comments", "task-run-1")
    assert spark.catalog.dropped_views == [spark.view_name]


def test_task_execution_persists_the_explicit_partial_status() -> None:
    spark = _FakeSpark()

    with TaskExecution(
        spark=spark,
        catalog="catalog",
        task_key="fetch_comments",
        task_run_id="task-run-1",
        ingestion_id="run-1",
    ) as execution:
        execution.complete(
            status="PARTIAL_SUCCESS",
            counts={
                "videos_attempted": 2,
                "videos_succeeded": 1,
                "videos_failed": 1,
                "records_fetched": 10,
            },
            api_cost_units=3,
        )

    assert spark.rows[0][5:11] == ("PARTIAL_SUCCESS", 2, 1, 1, 10, 3)


def test_task_execution_keeps_api_cost_when_the_task_fails() -> None:
    spark = _FakeSpark()

    with pytest.raises(RuntimeError, match="API failed"):
        with TaskExecution(
            spark=spark,
            catalog="catalog",
            task_key="fetch_comments",
            task_run_id="task-run-1",
            ingestion_id="run-1",
        ) as execution:
            execution.add_api_cost(2)
            raise RuntimeError("API failed")

    assert spark.rows[0][5] == "FAILED"
    assert spark.rows[0][10] == 2

