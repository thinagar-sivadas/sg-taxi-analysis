"""Consumer to consume taxi availability data"""

from dagster import (
    AutoMaterializePolicy,
    AutoMaterializeRule,
    asset,
    get_dagster_logger,
)
from data_pipeline.assets.consumers.consumer import Consumer

LOCAL_TIMEZONE = "Asia/Singapore"


@asset(
    code_version="v0.0.1",
    description="Consumer to consume taxi availability data",
    compute_kind="python",
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=5).with_rules(
        AutoMaterializeRule.materialize_on_cron(
            cron_schedule="*/1 * * * *",
            timezone=LOCAL_TIMEZONE,
            all_partitions=False,
        )
    ),
    owners=["thinagarsivadas@gmail.com"],
)
def consume_taxi_availability() -> None:
    """Consumer to consume taxi availability data"""

    consumer = Consumer(
        topic_name="TaxiAvailability",
        consumer_group="GetTaxiAvailability-Datalake",
        logger=get_dagster_logger(),
        auto_offset_reset="earliest",
    )
    consumer.consume()
