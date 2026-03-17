from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    EnvVar,
    RunRequest,
    load_assets_from_modules,
    sensor,
)
from dotenv import load_dotenv

from .assets import spot_prices
from .assets.spot_prices import db_setup_job
from .resources import ApiResource, PostgresResource

load_dotenv()

all_assets = load_assets_from_modules([spot_prices])

automation_sensor = AutomationConditionSensorDefinition(
    name="entsoe_automation_sensor",
    target=AssetSelection.all(),
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=600,
)


@sensor(job=db_setup_job, default_status=DefaultSensorStatus.RUNNING, minimum_interval_seconds=3600)
def db_setup_sensor():
    yield RunRequest(
        run_key="db_setup_once",
        run_config={"ops": {"setup_db_op": {"config": {"vat_percentage": 25.5}}}},
    )


defs = Definitions(
    assets=all_assets,
    jobs=[db_setup_job],
    sensors=[automation_sensor, db_setup_sensor],
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
