"""API module for taxi availability data retrieval"""

import asyncio
import datetime
import json
import logging
from dataclasses import dataclass
from datetime import time

import aiohttp
import pandas as pd
from data_pipeline.assets.producers.producer import Producer
from dateutil import tz


@dataclass
class TaxiAvailability:
    """TaxiAvailability class for taxi availability data retrieval"""

    date: str
    logger: logging.Logger
    max_coroutine: int = 10
    local_timezone: str = "Asia/Singapore"
    producer: Producer = None

    async def generate_date_time_interval(self) -> list[str]:
        """Generate date time interval for the given date"""

        date_to_process = datetime.datetime.strptime(self.date, "%Y-%m-%d").date()

        start_date_time = datetime.datetime.combine(date_to_process, time.min)
        end_date_time = datetime.datetime.combine(date_to_process, time.max)

        intervals = [
            dttm.strftime("%Y-%m-%dT%H:%M:%S") for dttm in pd.date_range(start_date_time, end_date_time, freq="1min")
        ]

        return intervals

    async def get_date_time(self) -> list[str]:
        """Get date time for the given date"""

        date_time_to_process = datetime.datetime.strptime(self.date, "%Y-%m-%d").replace(
            tzinfo=tz.gettz(self.local_timezone)
        )
        current_date_time = datetime.datetime.now().astimezone(tz.gettz(self.local_timezone))

        if date_time_to_process.date() < current_date_time.date():
            date_time = await self.generate_date_time_interval()
        else:
            date_time = [pd.Timestamp(current_date_time).floor("1min").strftime("%Y-%m-%dT%H:%M:%S")]

        return date_time

    async def get_request(
        self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, date_time: str, coroutine: int
    ) -> None:
        """Get request for the given date time"""

        async with semaphore:
            self.logger.info(
                "[Coroutine %s] Retrieving taxi availability data for %s -> Starting",
                coroutine,
                date_time,
            )
            response = await session.get(
                "https://api.data.gov.sg/v1/transport/taxi-availability", params={"date_time": date_time}, timeout=300
            )

            if response.status != 200:
                self.logger.info(
                    "[Coroutine %s] Retrieving taxi availability data for %s -> "
                    + "Unsuccessful [Status code: %s, Reason: %s, URL: %s]",
                    coroutine,
                    date_time,
                    response.status,
                    response.reason,
                    response.url,
                )
                # Function Send data to kafka dlq
            else:
                response = await response.json()
                self.logger.info(
                    "[Coroutine %s] Retrieving taxi availability data for %s -> Completed",
                    coroutine,
                    date_time,
                )
                if self.producer:
                    self.producer.produce(
                        key=date_time.split("T")[0],
                        value=json.dumps(response),
                        custom_message=f"[Coroutine {coroutine}] ",
                    )

    async def retrieve_response(self, date_time_list: list[str]) -> None:
        """Retrieve response for the given date time list"""

        semaphore = asyncio.Semaphore(self.max_coroutine)
        async with aiohttp.ClientSession(
            headers={"content-type": "application/json"}, raise_for_status=False
        ) as session:
            coroutine_request_list = [
                self.get_request(
                    session=session,
                    semaphore=semaphore,
                    date_time=date_time,
                    coroutine=ind + 1,
                )
                for ind, date_time in enumerate(date_time_list)
            ]
            await asyncio.gather(*coroutine_request_list)

    async def retrieve_data(self) -> None:
        """Retrieve data for the given date"""

        date_time = await self.get_date_time()
        # Remove list slicing for production
        await self.retrieve_response(date_time[0:10])
