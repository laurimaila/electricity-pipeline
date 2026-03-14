from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    EnvVar,
    load_assets_from_modules,
)
from dotenv import load_dotenv

from .assets import prices
from .resources import ApiResource, PostgresResource

load_dotenv()

all_assets = load_assets_from_modules([prices])

automation_sensor = AutomationConditionSensorDefinition(
    name="entsoe_automation_sensor",
    target=AssetSelection.all(),
    default_status=DefaultSensorStatus.RUNNING,
)

defs = Definitions(
    assets=all_assets,
    sensors=[automation_sensor],
    resources={
        "entsoe": ApiResource(
            api_key=EnvVar("ENTSOE_API_TOKEN"),
        ),
        "postgres": PostgresResource(
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar("POSTGRES_PORT"),
            db_name=EnvVar("POSTGRES_DB"),
        ),
    },
)
