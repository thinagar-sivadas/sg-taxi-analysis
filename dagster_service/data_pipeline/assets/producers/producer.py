"""Producer module to produce messages to a Kafka topic"""

import logging
from dataclasses import dataclass, field
from functools import partial

from quixstreams import Application, MessageContext
from quixstreams.models import TopicConfig


@dataclass
class Producer:
    """Producer class to produce messages to a Kafka topic"""

    topic_name: str
    logger: logging.Logger
    broker_address: str = "redpanda:9092"
    num_partitions: int = 5
    replication_factor: int = 1
    app: Application = field(init=False, default=None)

    def __post_init__(self):
        self.start_app()

    def start_app(self):
        """Start the application with the given broker address and topic configuration"""

        self.app = Application(broker_address=self.broker_address, auto_create_topics=True, loglevel=None)
        self.app.topic(
            name=self.topic_name,
            config=TopicConfig(replication_factor=self.replication_factor, num_partitions=self.num_partitions),
        )

    def delivery_report(self, err: BaseException | None, msg: MessageContext, custom_message: str = ""):
        """Delivery report callback to log the delivery status of the message"""

        if err is not None:
            self.logger.error("%s Message delivery failed: %s", custom_message, err)
        else:
            self.logger.info(
                "%s Message delivered to -> (Topic: %s, Partition: %s, Key: %s, Offset: %s)",
                custom_message,
                msg.topic(),
                msg.partition(),
                msg.key(),
                msg.offset(),
            )

    def produce(self, value: str, write_to_partition: int = None, key: str = None, custom_message: str = ""):
        """Produce message to the Kafka topic with the given value, partition and key"""

        try:
            with self.app.get_producer() as producer:
                producer.produce(
                    topic=self.topic_name,
                    partition=write_to_partition,
                    key=key,
                    value=value,
                    on_delivery=partial(self.delivery_report, custom_message=custom_message),
                )
        except Exception as e:
            self.logger.error("An error occurred while producing message: %s", e)
            raise e
