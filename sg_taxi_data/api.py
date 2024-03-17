"""This module contains the code to hit the Singapore taxi availability
API and retrieve the data for the date_time provided"""

import argparse
import asyncio
import datetime
from datetime import time, timedelta

import aiohttp
import pandas as pd
from dateutil import tz

from sg_taxi_data.model import FeatureCollection

current_timezone = tz.gettz("Asia/Singapore")


async def get_interval(date_time: str):
    """Get the start and end interval for the date_time provided"""

    if len(date_time) == 16:
        process_date_time = datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M")
        start_date_time = process_date_time - timedelta(minutes=5)
        end_date_time = process_date_time
        intervals = [
            dttm.strftime("%Y-%m-%dT%H:%M:%S")
            for dttm in pd.date_range(start_date_time, end_date_time, freq="1min", inclusive="right")
        ]

    else:
        current_date_time = datetime.datetime.now(datetime.UTC).astimezone(current_timezone)

        if date_time == "today":
            process_date = datetime.datetime.now(datetime.UTC).astimezone(current_timezone).date()
        else:
            process_date = datetime.datetime.strptime(date_time, "%Y-%m-%d").date()

        start_date_time = datetime.datetime.combine(process_date, time.min)

        if process_date < current_date_time.date():
            end_date_time = datetime.datetime.combine(process_date, time.max)
        else:
            end_date_time = current_date_time.replace(tzinfo=None)

        intervals = [
            dttm.strftime("%Y-%m-%dT%H:%M:%S") for dttm in pd.date_range(start_date_time, end_date_time, freq="1min")
        ]

    return intervals


async def send_request(session: aiohttp.ClientSession, interval: str, version: str):
    """Send request to the API for the taxi availability data for the interval provided"""

    print(f"Sending request for {interval}")
    response = await session.get(
        f"https://api.data.gov.sg/{version}/transport/taxi-availability?date_time={interval}",
        raise_for_status=True,
        timeout=300,
    )
    print(f"Request for {interval} successful")
    return response


async def get_data(date_time: str = "today", version: str = "v1"):
    """Get the taxi availability data for the date_time provided"""

    async with aiohttp.ClientSession() as session:
        intervals = await get_interval(date_time)
        tasks = [send_request(session, interval, version) for interval in intervals]
        responses = await asyncio.gather(*tasks)
        responses = [await response.json() for response in responses]

    data = {"features": []}
    for response in responses:
        data["features"].extend(response["features"])

    return FeatureCollection(**data)


def validate_date(date_str: str):
    """Validate the date_time format provided by the user"""

    if date_str == "today":
        return date_str
    try:
        if len(date_str) == 16:
            datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M")
        elif len(date_str) == 10:
            datetime.datetime.strptime(date_str, "%Y-%m-%d")
        else:
            raise ValueError
        return date_str
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Invalid date_time format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM") from exc


def main():
    """Main function to parse the arguments and run the code to get the taxi availability data"""

    parser = argparse.ArgumentParser(description="Hits the sg taxi availability API")
    parser.add_argument(
        "--date_time",
        help="Use the date_time parameter to retrieve the latest available data at that moment in time "
        + "[YYYY-MM-DD or YYYY-MM-DD HH:MM]. Defaults to 'today'",
        default="today",
        nargs="?",
        metavar="",
        required=False,
        type=validate_date,
    )
    args = parser.parse_args()
    return asyncio.run(get_data(date_time=args.date_time))


if __name__ == "__main__":

    main()
