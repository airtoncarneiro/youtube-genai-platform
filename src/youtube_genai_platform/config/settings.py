from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:

    CATALOG = "youtube_genai"

    CONTROL_SCHEMA = "control"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"

    CHANNELS_TABLE = (
        f"{CATALOG}.{CONTROL_SCHEMA}.youtube_channels"
    )

    WATERMARKS_TABLE = (
        f"{CATALOG}.{CONTROL_SCHEMA}.youtube_watermarks"
    )

    PIPELINE_RUNS_TABLE = (
        f"{CATALOG}.{CONTROL_SCHEMA}.youtube_pipeline_runs"
    )


settings = Settings()
