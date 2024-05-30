"""Producer to generate taxi availability data"""

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    BackfillPolicy,
    DailyPartitionsDefinition,
    asset,
)
from data_pipeline.assets.producer.taxi_availability.api import TaxiAvailability


@asset(
    code_version="v0.0.1",
    description="Producer to generate taxi availability data",
    compute_kind="python",
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=1).with_rules(
        AutoMaterializeRule.materialize_on_cron(cron_schedule="*/5 * * * *", timezone="Asia/Singapore")
    ),
    partitions_def=DailyPartitionsDefinition(start_date="2024-03-01", timezone="Asia/Singapore", end_offset=1),
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
    owners=["thinagarsivadas@gmail.com"],
)
def generate_taxi_availability(context: AssetExecutionContext) -> None:
    """Generate taxi availability data"""

    _ = TaxiAvailability(date="2024-06-30")
    context.log.info(f"Running for partition {context.partition_key_range}")
    print("asd")
