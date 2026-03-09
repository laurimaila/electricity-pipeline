from dotenv import load_dotenv
from dagster import Definitions, load_assets_from_modules, EnvVar, AutomationConditionSensorDefinition, DefaultSensorStatus, AssetSelection
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
            connection_string=EnvVar("POSTGRES_CONNECTION_STRING"),
        ),
    },
)
