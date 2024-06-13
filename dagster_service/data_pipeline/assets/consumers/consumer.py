"""Consumer model to consume messages from a kafka topic"""

import json
import logging
from dataclasses import dataclass, field

from quixstreams import Application


@dataclass
class Consumer:
    """Consumer class to consume messages from a Kafka topic"""

    topic_name: str
    consumer_group: str
    logger: logging.Logger
    timeout: float = 1.5
    auto_offset_reset: str = "earliest"
    broker_address: str = "redpanda:9092"
    app: Application = field(init=False, default=None)

    def __post_init__(self):
        self.start_app()

    def start_app(self):
        """Start the application with the given broker address and topic configuration"""

        self.app = Application(
            broker_address=self.broker_address,
            consumer_group=self.consumer_group,
            auto_offset_reset=self.auto_offset_reset,
            loglevel=None,
        )

    def consume(self):
        """Consume messages from the Kafka topic"""

        try:
            with self.app.get_consumer() as consumer:
                consumer.subscribe([self.topic_name])
                while True:
                    msg = consumer.poll(self.timeout)

                    if msg is None:
                        self.logger.info("No message to consume")
                        break

                    if msg.error() is not None:
                        raise ValueError(msg.error())

                    _ = json.loads(msg.value())
                    consumer.store_offsets(message=msg)
                    self.logger.info(
                        "Consumed message from -> (Topic: %s, Partition: %s, Key: %s, Offset: %s)",
                        msg.topic(),
                        msg.partition(),
                        msg.key().decode("utf-8") if msg.key() else None,
                        msg.offset(),
                    )
        except Exception as e:
            self.logger.error("An error occurred while consuming message: %s", e)
            raise e
