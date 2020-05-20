import asyncio
import collections
import logging
import re
import struct
from io import BytesIO
from typing import Any, Callable, List, Tuple, Union

import confluent_kafka
from confluent_avro import SchemaRegistry

from . import utils
from .consumer import AsyncKafkaConsumer
from .models import SchematicSerializable, Serializable
from .producer import AsyncKafkaProducer


class KafkaStreamer:
    _MAGIC_BYTE = 0

    def __init__(self,
                 hosts: str,
                 group_id: str,
                 schema_registry_url: str = None,
                 logger: logging.Logger = None,
                 debug: bool = False):
        self._hosts = hosts
        self._group_id = group_id
        self._parent_logger = logger
        self._debug = debug
        self._simple_selectors = collections.defaultdict(list)
        self._regex_selectors = collections.defaultdict(list)
        self.logger = logger if logger is not None else logging.getLogger(
            'KafkaStreamer')
        self._registry = SchemaRegistry(
            schema_registry_url,
            headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'
                     }) if schema_registry_url is not None else None

    async def __aenter__(self):
        self._create_producer()
        self._create_consumer()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type != asyncio.CancelledError:
            self._consumer_task.cancel()
            self._producer_task.cancel()
        await asyncio.gather(self._producer_task,
                             self._consumer_task,
                             return_exceptions=True)

    def _create_producer(self):
        self._kafka_producer = AsyncKafkaProducer(self._hosts,
                                                  logger=self._parent_logger,
                                                  debug=self._debug)
        self._producer_queue = asyncio.Queue()
        self._producer_task = asyncio.create_task(
            self._kafka_producer.queue_to_kafka(self._producer_queue))

    def _create_consumer(self, queue_max_size: int = 1000):
        unique_topics = self._unique_topics()
        if len(unique_topics) == 0:
            raise RuntimeError('No subscription set.')
        self._kafka_consumer = AsyncKafkaConsumer(self._hosts,
                                                  self._group_id,
                                                  unique_topics,
                                                  False,
                                                  logger=self._parent_logger,
                                                  debug=self._debug)
        self._consumer_queue = asyncio.Queue(queue_max_size)
        self._consumer_task = asyncio.create_task(
            self._kafka_consumer.kafka_to_queue(self._consumer_queue))

    def _unique_topics(self):
        simple_topics = list(self._simple_selectors.keys())
        regex_topics = [
            regex.pattern for regex in self._regex_selectors.keys()
        ]
        return list(set(simple_topics + regex_topics))

    def topic(self, topic_selector: str):
        def decorator(func):
            func_async = utils.async_wrap(func)
            utils.raise_if_function_has_multiple_parameters(func_async)
            self._add_callback_to_selector(
                func_async, topic_selector,
                utils.get_first_parameter_type_of_function(func_async))
            return func_async

        return decorator

    def _add_callback_to_selector(self, func, topic_selector, serializer):
        if topic_selector.startswith('^'):
            pattern = re.compile(topic_selector)
            self._regex_selectors[pattern].append((func, serializer))
        else:
            self._simple_selectors[topic_selector].append((func, serializer))

    async def run(self):
        async with self:
            while True:
                msg: confluent_kafka.Message = await self._consumer_queue.get()
                await asyncio.gather(*self._message_handlers(msg))
                self._kafka_consumer.set_offset(msg)

    def _message_handlers(self, msg: confluent_kafka.Message) -> list:
        return [
            consumer_callback(self._deserialize(msg, serializable))
            for consumer_callback, serializable in self._get_topic_consumers(
                msg.topic())
        ]

    def _get_topic_consumers(self,
                             topic_name: str) -> List[Tuple[Callable, Any]]:
        consumers = []
        if topic_name in self._simple_selectors:
            consumers.extend(self._simple_selectors[topic_name])
        for pattern in self._regex_selectors:
            if pattern.match(topic_name):
                consumers.extend(self._regex_selectors[pattern])
        return consumers

    async def send(self,
                   topic: str,
                   value: Union[bytes, Serializable, SchematicSerializable],
                   key: bytes = None):
        await self._producer_queue.put(self._serialize(topic, value, key))

    def _deserialize(self, message: confluent_kafka.Message,
                     deserialize_type: Union[None, confluent_kafka.Message,
                                             Serializable,
                                             SchematicSerializable]):
        if deserialize_type is None or \
           deserialize_type == confluent_kafka.Message:
            return message
        elif issubclass(deserialize_type, SchematicSerializable):
            with BytesIO(message.value()) as in_stream:
                magic, schema_id = struct.unpack(
                    ">bI", in_stream.read(self._registry.schema_id_size + 1))
                if magic != self._MAGIC_BYTE:
                    raise TypeError("message does not start with magic byte")
                return deserialize_type.from_bytes(
                    in_stream, schema_id, self._registry.get_schema(schema_id))
        elif issubclass(deserialize_type, Serializable):
            return deserialize_type.from_bytes(message.value())

    def _serialize(self,
                   topic: str,
                   value: Union[bytes, Serializable, SchematicSerializable],
                   key: bytes = None) -> dict:
        if isinstance(value, SchematicSerializable):
            subject = f"{topic}-value"
            schema_id = self._registry.register_schema(subject, value._schema)
            with BytesIO() as out_stream:
                out_stream.write(struct.pack("b", self._MAGIC_BYTE))
                out_stream.write(struct.pack(">I", schema_id))
                value = value.to_bytes(out_stream)
        elif isinstance(value, Serializable):
            value = value.to_bytes()
        return {'topic': topic, 'key': key, 'value': value}
