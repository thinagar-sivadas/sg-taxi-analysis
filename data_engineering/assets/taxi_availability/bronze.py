"""Get taxi availability data from the API and store it in the bronze bucket"""

import asyncio
import datetime
import io
import warnings

import boto3
from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    BackfillPolicy,
    DailyPartitionsDefinition,
    ExperimentalWarning,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from dateutil import tz

from sg_taxi_data.api import get_data

warnings.filterwarnings("ignore", category=ExperimentalWarning)
current_timezone = tz.gettz("Asia/Singapore")


@asset(
    code_version="v0.0.1",
    compute_kind="api",
    auto_materialize_policy=AutoMaterializePolicy.eager(max_materializations_per_minute=1).with_rules(
        AutoMaterializeRule.materialize_on_cron(cron_schedule="*/5 * * * *", timezone="Asia/Singapore")
    ),
    partitions_def=DailyPartitionsDefinition(start_date="2024-03-01", timezone="Asia/Singapore", end_offset=1),
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
    retry_policy=RetryPolicy(max_retries=1, delay=5),
    owners=["thinagarsivadas@gmail.com"],
)
def taxi_availability(context: AssetExecutionContext) -> None:
    """Get taxi availability data from the API and store it in the bronze bucket"""

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio-datalake:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
    )

    context.log.info(f"Running for partition {context.partition_key}")
    process_partition_date_time = datetime.datetime.strptime(context.partition_key, "%Y-%m-%d")
    process_date_time = datetime.datetime.fromtimestamp(
        timestamp=context.instance.get_run_stats(context.run_id).start_time, tz=current_timezone
    )

    if process_partition_date_time.date() < process_date_time.date():
        process_date_time = context.partition_key
        context.log.info(f"Getting taxi availability for {process_date_time}")
    else:
        process_date_time = process_date_time.strftime("%Y-%m-%d %H:%M")
        context.log.info(f"Getting last 5 minutes of taxi availability from {process_date_time}")

    data = asyncio.run(get_data(date_time=process_date_time))
    for feature in data.features:
        timestamp = feature.properties.timestamp
        date = timestamp.split("T")[0]
        serialise_data = io.BytesIO(feature.model_dump_json().encode("utf-8"))
        s3.put_object(Bucket="bronze", Body=serialise_data, Key=f"taxi_availability/date={date}/{timestamp}.json")

    return MaterializeResult(
        metadata={
            "num_json_files_created": len(data.features),
            "schema": MetadataValue.json(data.features[0].model_json_schema()["$defs"]),
        }
    )
