import os

from dagster import (
    AutomationCondition,
    Config,
    DailyPartitionsDefinition,
)


class PriceConfig(Config):
    vat_percentage: float = float(os.getenv("VAT_PERCENTAGE", 25.5))


# 24h CET partitions starting from October 2nd 2025
price_partitions = DailyPartitionsDefinition(
    start_date="2025-10-02", timezone="Europe/Brussels", end_offset=2
)

# Re-run if data is missing or downstream failed
entsoe_automation_condition = (
    AutomationCondition.missing() | AutomationCondition.any_deps_missing()
) & AutomationCondition.on_cron("*/10 12-14 * * *")
