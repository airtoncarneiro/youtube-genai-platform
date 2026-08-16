from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_PATH = ROOT / "src/dashboards/youtube_channel_discovery.lvdash.json"
RESOURCE_PATH = ROOT / "resources/youtube_channel_discovery.dashboard.yml"


def _dashboard() -> dict:
    return json.loads(DASHBOARD_PATH.read_text(encoding="utf-8"))


def _queries() -> dict[str, str]:
    return {
        dataset["name"]: "".join(dataset["queryLines"])
        for dataset in _dashboard()["datasets"]
    }


def test_dashboard_uses_the_channel_discovery_control_tables() -> None:
    queries = _queries()

    assert "FROM channel_targets" in queries["ds_channel_targets"]
    assert "discovery_mode" in queries["ds_channel_targets"]
    assert (
        "WHEN 'ALL' THEN 2 WHEN 'LAST' THEN 1 ELSE 0 END"
        in queries["ds_channel_targets"]
    )
    assert "FROM channel_discovery_runs" in queries["ds_discovery_runs"]
    assert "date_sub(current_date(), 90)" in queries["ds_discovery_runs"]
    assert "youtube_lakehouse." not in "\n".join(queries.values())


def test_dashboard_exposes_true_channel_and_video_metrics() -> None:
    target_dataset = next(
        dataset
        for dataset in _dashboard()["datasets"]
        if dataset["name"] == "ds_channel_targets"
    )
    discovery_dataset = next(
        dataset
        for dataset in _dashboard()["datasets"]
        if dataset["name"] == "ds_discovery_runs"
    )
    expressions = {
        column["displayName"]: column["expression"]
        for column in discovery_dataset["columns"]
    }

    assert expressions["Vídeos cadastrados"] == "SUM(`videos_registered`)"
    assert expressions["Falhas de canal"] == "SUM(`channels_failed`)"
    assert expressions["Custo estimado da API"] == "SUM(`api_cost_units`)"
    assert target_dataset["columns"] == [
        {
            "displayName": "Canais com descoberta ativa",
            "description": "Quantidade de canais configurados nos modos ALL ou LAST",
            "expression": "SUM(CASE WHEN `discovery_mode` <> 'NONE' THEN 1 ELSE 0 END)",
        }
    ]


def test_dashboard_layout_has_no_overlaps_and_uses_grid_v1() -> None:
    for page in _dashboard()["pages"]:
        assert page["layoutVersion"] == "GRID_V1"
        occupied: set[tuple[int, int]] = set()
        for item in page["layout"]:
            position = item["position"]
            cells = {
                (x, y)
                for x in range(position["x"], position["x"] + position["width"])
                for y in range(position["y"], position["y"] + position["height"])
            }
            assert not occupied.intersection(cells), item["widget"]["name"]
            occupied.update(cells)


def test_dashboard_resource_uses_control_schema_and_target_catalog() -> None:
    resource = RESOURCE_PATH.read_text(encoding="utf-8")

    assert 'display_name: "YouTube - Descoberta de vídeos"' in resource
    assert "dataset_catalog: ${var.catalog}" in resource
    assert "dataset_schema: control" in resource
