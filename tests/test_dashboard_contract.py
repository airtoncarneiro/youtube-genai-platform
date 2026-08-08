import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_PATH = ROOT / "src/dashboards/youtube_operational.lvdash.json"
RESOURCE_PATH = ROOT / "resources/youtube_operational.dashboard.yml"
SETUP_NOTEBOOK_PATH = ROOT / "src/notebooks/00_setup.ipynb"


def _dashboard() -> dict:
    return json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))


def _queries_by_dataset() -> dict[str, str]:
    return {
        dataset["name"]: "".join(dataset["queryLines"])
        for dataset in _dashboard()["datasets"]
    }


def _setup_sql() -> str:
    notebook = json.loads(SETUP_NOTEBOOK_PATH.read_text(encoding="utf-8"))
    return "".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell["id"] in {"setup-control-ddl", "setup-raw-ddl", "setup-silver-ddl"}
    )


def test_every_widget_references_an_existing_dataset() -> None:
    dashboard = _dashboard()
    dataset_names = {dataset["name"] for dataset in dashboard["datasets"]}

    referenced_names = {
        query["query"]["datasetName"]
        for page in dashboard["pages"]
        for layout in page["layout"]
        for query in layout["widget"].get("queries", [])
    }

    assert referenced_names <= dataset_names
    assert referenced_names == dataset_names


def test_widget_positions_do_not_overlap() -> None:
    for page in _dashboard()["pages"]:
        occupied: set[tuple[int, int]] = set()
        for layout in page["layout"]:
            position = layout["position"]
            cells = {
                (x, y)
                for x in range(position["x"], position["x"] + position["width"])
                for y in range(position["y"], position["y"] + position["height"])
            }
            assert not occupied.intersection(cells), layout["widget"]["name"]
            occupied.update(cells)


def test_operational_and_trend_windows_are_bounded() -> None:
    queries = _queries_by_dataset()

    assert "INTERVAL 30 DAYS" in queries["ds_runs"]
    assert "LIMIT 500" in queries["ds_runs"]
    assert "INTERVAL 89 DAYS" in queries["ds_video_trend"]
    assert "INTERVAL 89 DAYS" in queries["ds_channel_trend"]


def test_trends_use_active_entities_and_last_observation_carried_forward() -> None:
    queries = _queries_by_dataset()

    for name in ("ds_video_trend", "ds_channel_trend"):
        query = queries[name]
        assert "WHERE targets.is_active" in query or "WHERE is_active" in query
        assert "CROSS JOIN" in query
        assert "MAX_BY" in query
        assert "metrics.collected_at < dates.collected_date + INTERVAL 1 DAY" in query


def test_failed_counter_only_counts_active_targets() -> None:
    targets = next(
        dataset for dataset in _dashboard()["datasets"] if dataset["name"] == "ds_targets"
    )
    failed = next(
        column for column in targets["columns"] if column["displayName"] == "Vídeos com falha"
    )

    assert "`is_active` AND `status` = 'FAILED'" in failed["expression"]


def test_latest_video_metrics_only_include_active_targets() -> None:
    query = _queries_by_dataset()["ds_latest_videos"]

    assert "INNER JOIN dashboard_video_targets" in query
    assert "WHERE targets.is_active" in query
    assert "SELECT metrics.video_id AS video_id" in query
    assert "PARTITION BY metrics.video_id" in query


def test_dashboard_resource_matches_the_setup_catalog_and_schema() -> None:
    resource = RESOURCE_PATH.read_text(encoding="utf-8")
    setup = SETUP_NOTEBOOK_PATH.read_text(encoding="utf-8")

    assert "dataset_catalog: youtube_lakehouse" in resource
    assert "dataset_schema: silver" in resource
    assert "youtube_lakehouse.silver.dashboard_video_metrics" in setup
    assert "youtube_lakehouse.silver.dashboard_channel_metrics" in setup


def test_every_physical_table_column_has_a_comment_in_the_setup_notebook() -> None:
    setup = _setup_sql()
    table_definitions = re.finditer(
        r"CREATE TABLE IF NOT EXISTS ([\w.]+) \((.*?)\) USING DELTA",
        setup,
        flags=re.DOTALL,
    )

    for definition in table_definitions:
        table_name, columns_sql = definition.groups()
        for line in columns_sql.splitlines():
            column = re.match(r"\s{2}([A-Za-z_]\w*)\s+", line)
            if column and column.group(1) != "CONSTRAINT":
                assert f"COMMENT ON COLUMN {table_name}.{column.group(1)}" in setup


def test_dashboard_ingestion_duration_has_a_comment_in_the_setup_notebook() -> None:
    setup = _setup_sql()

    assert (
        "COMMENT ON COLUMN "
        "youtube_lakehouse.silver.dashboard_ingestion_runs.duration_seconds "
        "IS 'Duração da execução em segundos, calculada como diferença entre "
        "ended_at e started_at'"
    ) in setup


def test_key_columns_are_documented_for_genie() -> None:
    setup = _setup_sql()
    expected_prefixes = {
        "channels.channel_id": "PK:",
        "videos.video_id": "PK:",
        "videos.channel_id": "FK → channels.channel_id.",
        "video_tags.video_id": "PK (composta com tag), FK → videos.video_id.",
        "video_tags.tag": "PK (composta com video_id).",
        "comments.comment_id": "PK:",
        "comments.parent_id": "FK → comments.comment_id (recursivo).",
        "comments.video_id": "FK → videos.video_id.",
        "replies.comment_id": "PK:",
        "replies.parent_id": "FK → comments.comment_id.",
        "replies.video_id": "FK → videos.video_id.",
        "channel_snapshots.channel_id": (
            "PK (lógica, composta com ingestion_id), FK → channels.channel_id."
        ),
        "channel_snapshots.ingestion_id": "PK lógica (composta com channel_id).",
        "video_snapshots.video_id": (
            "PK (lógica, composta com ingestion_id), FK → videos.video_id."
        ),
        "video_snapshots.ingestion_id": "PK lógica (composta com video_id).",
    }

    for column, prefix in expected_prefixes.items():
        assert (
            f"COMMENT ON COLUMN youtube_lakehouse.silver.{column} IS '{prefix}"
        ) in setup
