"""Producer to generate taxi availability data"""

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    BackfillPolicy,
    DailyPartitionsDefinition,
    asset,
)
from data_pipeline.assets.producer.taxi_availability_api import TaxiAvailability


@asset(
    code_version="v0.0.1",
    description="Producer to generate taxi availability data",
    compute_kind="python",
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=1).with_rules(
        AutoMaterializeRule.materialize_on_cron(
            cron_schedule="*/1 * * * *",
            timezone="Asia/Singapore",
            all_partitions=False,
        )
    ),
    partitions_def=DailyPartitionsDefinition(start_date="2024-05-01", timezone="Asia/Singapore", end_offset=1),
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=5),
    owners=["thinagarsivadas@gmail.com"],
)
async def generate_taxi_availability(context: AssetExecutionContext) -> None:
    """Producer to generate taxi availability data"""

    taxi_availability = TaxiAvailability(
        date=context.partition_key_range.start,
        context=context,
        max_coroutine=10,
    )
    await taxi_availability.retrieve_data()
