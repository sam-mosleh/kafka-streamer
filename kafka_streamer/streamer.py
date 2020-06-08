import asyncio
import logging
from typing import List, Optional, Union

import confluent_kafka
from confluent_avro import SchemaRegistry

from kafka_streamer.client import AsyncKafkaConsumer, AsyncKafkaProducer
from kafka_streamer.models import SchematicSerializable, Serializable
from kafka_streamer.topic import RegexTopic, SingleTopic


class KafkaStreamer:
    def __init__(
        self,
        hosts: List[str],
        group_id: str,
        schema_registry_url: str = None,
        queue_max_size: int = 1000,
        logger: Optional[logging.Logger] = None,
        debug: bool = False,
    ):
        self._hosts = hosts
        self._group_id = group_id
        self._parent_logger = logger
        self._debug = debug
        self._topics: List[Union[SingleTopic, RegexTopic]] = []
        self.logger = logger or logging.getLogger("KafkaStreamer")
        self._registry = (
            SchemaRegistry(
                schema_registry_url,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )
            if schema_registry_url is not None
            else None
        )
        self._queue_max_size = queue_max_size

    async def __aenter__(self):
        await self._create_producer()
        await self._create_consumer()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type != asyncio.CancelledError:
            self._consumer_task.cancel()
            self._producer_task.cancel()
        await asyncio.gather(
            self._producer_task, self._consumer_task, return_exceptions=True
        )

    async def _create_producer(self):
        self._kafka_producer = AsyncKafkaProducer(
            self._hosts, logger=self._parent_logger, debug=self._debug
        )
        self._producer_queue = asyncio.Queue()
        self._producer_task = asyncio.create_task(
            self._kafka_producer.queue_to_kafka(self._producer_queue)
        )

    async def _create_consumer(self):
        self._kafka_consumer = AsyncKafkaConsumer(
            self._hosts,
            self._group_id,
            self._unique_consuming_topics(),
            False,
            logger=self._parent_logger,
            debug=self._debug,
        )
        self._consumer_queue = asyncio.Queue(self._queue_max_size)
        self._consumer_task = asyncio.create_task(
            self._kafka_consumer.kafka_to_queue(self._consumer_queue)
        )

    def _unique_consuming_topics(self) -> List[str]:
        return list(set([topic.name for topic in self._topics if topic.has_consumer()]))

    def topic(
        self,
        topic_name: str,
        value_type: Union[SchematicSerializable, Serializable, bytes] = bytes,
        key_type: Union[SchematicSerializable, Serializable, bytes] = bytes,
    ) -> Union[RegexTopic, SingleTopic]:
        if topic_name.startswith("^"):
            topic = RegexTopic(
                topic_name,
                value_type=value_type,
                key_type=key_type,
                schema_registry=self._registry,
            )
        else:
            topic = SingleTopic(
                topic_name,
                produce_callback=self.send,
                value_type=value_type,
                key_type=key_type,
                schema_registry=self._registry,
            )
        self._topics.append(topic)
        return topic

    async def run(self):
        async with self:
            while True:
                msg: confluent_kafka.Message = await self._consumer_queue.get()
                all_handlers = [
                    topic.message_handlers(msg)
                    for topic in self._topics
                    if topic.match(msg.topic())
                ]
                await asyncio.gather(*all_handlers)
                self._kafka_consumer.set_offset(msg)

    async def send(
        self, topic: str, value: bytes, key: bytes = None,
    ):
        await self._producer_queue.put({"topic": topic, "key": key, "value": value})
