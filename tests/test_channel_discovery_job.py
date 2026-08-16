from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DISCOVERY_JOB_PATH = ROOT / "resources/youtube_channel_discovery.job.yml"
INGESTION_JOB_PATH = ROOT / "resources/youtube_ingestion.job.yml"


def test_channel_discovery_orchestrates_ingestion_after_discovery() -> None:
    discovery = DISCOVERY_JOB_PATH.read_text()

    assert "timeout_seconds: ${var.channel_discovery_job_timeout_seconds}" in discovery
    assert (
        """schedule:
        pause_status: UNPAUSED
        quartz_cron_expression: "0 0 18 * * ?"
        timezone_id: America/Sao_Paulo"""
        in discovery
    )
    assert (
        """- task_key: run_youtube_ingestion
          description: Executa a ingestão após a descoberta, inclusive quando a descoberta terminar com falha.
          depends_on:
            - task_key: discover_channel_videos
          run_if: ALL_DONE
          run_job_task:
            job_id: ${resources.jobs.youtube_ingestion.id}"""
        in discovery
    )


def test_youtube_ingestion_is_available_without_its_own_schedule() -> None:
    ingestion = INGESTION_JOB_PATH.read_text()

    assert "\n      trigger:\n" not in ingestion
