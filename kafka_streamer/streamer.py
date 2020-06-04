import asyncio
import logging
from typing import Any, Callable, List, Optional, Tuple, Union

import confluent_kafka
from confluent_avro import SchemaRegistry

from kafka_streamer.consumer import AsyncKafkaConsumer
from kafka_streamer.models import SchematicSerializable, Serializable
from kafka_streamer.producer import AsyncKafkaProducer


class KafkaStreamer:
    def __init__(
        self,
        hosts: List[str],
        group_id: str,
        schema_registry_url: str = None,
        queue_max_size: int = 1000,
        logger: logging.Logger = None,
        debug: bool = False,
    ):
        self._hosts = hosts
        self._group_id = group_id
        self._parent_logger = logger
        self._debug = debug
        self._topics: List[KafkaTopic] = []
        self.logger = (
            logger if logger is not None else logging.getLogger("KafkaStreamer")
        )
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
        return list(
            set([topic.topic_name for topic in self._topics if topic.has_consumer()])
        )

    def topic(
        self,
        topic_name: str,
        value_type: Union[SchematicSerializable, Serializable, bytes] = bytes,
        key_type: Union[SchematicSerializable, Serializable, bytes] = bytes,
    ) -> KafkaTopic:
        if topic_name.startswith("^"):
            topic = KafkaRegexTopic(
                topic_name,
                self.send,
                value_type,
                key_type,
                schema_registry=self._registry,
            )
        else:
            topic = KafkaSimpleTopic(
                topic_name,
                self.send,
                value_type,
                key_type,
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

    # def _deserialize(
    #     self,
    #     message: confluent_kafka.Message,
    #     deserialize_type: Union[
    #         None, confluent_kafka.Message, Serializable, SchematicSerializable
    #     ],
    # ):
    #     if deserialize_type is None or deserialize_type == confluent_kafka.Message:
    #         return message
    #     elif issubclass(deserialize_type, SchematicSerializable):
    #         with BytesIO(message.value()) as in_stream:
    #             magic, schema_id = struct.unpack(
    #                 ">bI", in_stream.read(self._registry.schema_id_size + 1)
    #             )
    #             if magic != self._MAGIC_BYTE:
    #                 raise TypeError("message does not start with magic byte")
    #             return deserialize_type.from_bytes(
    #                 in_stream, schema_id, self._registry.get_schema(schema_id)
    #             )
    #     elif issubclass(deserialize_type, Serializable):
    #         return deserialize_type.from_bytes(message.value())

    # def _serialize(
    #     self,
    #     topic: str,
    #     value: Union[bytes, Serializable, SchematicSerializable],
    #     key: bytes = None,
    # ) -> dict:
    #     if isinstance(value, SchematicSerializable):
    #         subject = f"{topic}-value"
    #         schema_id = self._registry.register_schema(subject, value._schema)
    #         with BytesIO() as out_stream:
    #             out_stream.write(struct.pack("b", self._MAGIC_BYTE))
    #             out_stream.write(struct.pack(">I", schema_id))
    #             value = value.to_bytes(out_stream)
    #     elif isinstance(value, Serializable):
    #         value = value.to_bytes()
    #     return {"topic": topic, "key": key, "value": value}
