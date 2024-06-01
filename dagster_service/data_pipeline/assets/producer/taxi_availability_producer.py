"""Producer to generate taxi availability data"""

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    BackfillPolicy,
    DailyPartitionsDefinition,
    asset,
    get_dagster_logger,
)
from data_pipeline.assets.producer.taxi_availability_api import TaxiAvailability

LOCAL_TIMEZONE = "Asia/Singapore"


@asset(
    code_version="v0.0.1",
    description="Producer to generate taxi availability data",
    compute_kind="python",
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=1).with_rules(
        AutoMaterializeRule.materialize_on_cron(
            cron_schedule="*/1 * * * *",
            timezone=LOCAL_TIMEZONE,
            all_partitions=False,
        )
    ),
    partitions_def=DailyPartitionsDefinition(start_date="2024-05-01", timezone=LOCAL_TIMEZONE, end_offset=1),
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
    owners=["thinagarsivadas@gmail.com"],
)
async def generate_taxi_availability(context: AssetExecutionContext) -> None:
    """Producer to generate taxi availability data"""

    taxi_availability = TaxiAvailability(
        date=context.partition_key_range.start,
        max_coroutine=10,
        logger=get_dagster_logger(),
        local_timezone=LOCAL_TIMEZONE,
    )
    await taxi_availability.retrieve_data()
