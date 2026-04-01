from dagster import (
    AssetSelection,
    AutomationCondition,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    EnvVar,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource
from dotenv import load_dotenv

from . import assets
from .assets.dbt import DBT_PROJECT_DIR, electricity_pipeline_dbt_assets
from .resources import ApiResource, PostgresResource

load_dotenv()

all_assets = load_assets_from_modules([assets])

# Apply eager() to all dbt assets
dbt_assets_with_automation = [
    electricity_pipeline_dbt_assets.map_asset_specs(
        lambda spec: spec.replace_attributes(automation_condition=AutomationCondition.eager())
    )
]

automation_sensor = AutomationConditionSensorDefinition(
    name="entsoe_automation_sensor",
    target=AssetSelection.all(),
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
)

defs = Definitions(
    assets=[*all_assets, *dbt_assets_with_automation],
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
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)
