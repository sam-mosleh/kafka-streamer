import asyncio
import collections
import logging
import re
from typing import Any, Callable, List, Tuple, Union

import confluent_kafka

from . import models, utils
from .consumer import KafkaConsumer
from .producer import KafkaProducer


class KafkaStreamer:
    def __init__(self,
                 hosts: str,
                 group_id: str,
                 logger: logging.Logger = None):
        self._simple_selectors = collections.defaultdict(list)
        self._regex_selectors = collections.defaultdict(list)
        self.logger = logger if logger is not None else logging.getLogger(
            'KafkaStreamer')
        self._kafka_producer = KafkaProducer(hosts, logger=logger)
        self._kafka_consumer = KafkaConsumer(hosts, group_id, logger=logger)

    async def __aenter__(self):
        self._kafka_consumer.subscription = self._unique_topics()
        self._producer_queue = asyncio.Queue()
        self._consumer_queue = asyncio.Queue(maxsize=2)
        self._producer_task = asyncio.create_task(
            self._kafka_producer.queue_to_kafka(self._producer_queue))
        self._consumer_task = asyncio.create_task(
            self._kafka_consumer.kafka_to_queue(self._consumer_queue))
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._consumer_task.cancel()
        self._producer_task.cancel()
        # await asyncio.gather(self._producer_task, self._consumer_task, return_exception=True)
        return self

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
                all_callback_results = await self._call_message_handlers(msg)
                for callback_result in all_callback_results:
                    if isinstance(callback_result, list):
                        for item in callback_result:
                            await self._producer_queue.put(item)
                    else:
                        await self._producer_queue.put(callback_result)

    async def _call_message_handlers(self, msg: confluent_kafka.Message):
        tasks = [
            self._deserialize_and_pass_to_function(consumer_callback,
                                                   serializable, msg)
            for consumer_callback, serializable in self._get_topic_consumers(
                msg.topic())
        ]
        return [
            callback_result for callback_result in await asyncio.gather(*tasks)
            if callback_result is not None
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

    def _deserialize_and_pass_to_function(
            self, func: Callable,
            deserialize_type: Union[None, confluent_kafka.Message, models.
                                    SerializableObject],
            message: confluent_kafka.Message):
        if deserialize_type is None or \
           deserialize_type == confluent_kafka.Message:
            return func(message)
        else:
            return func(deserialize_type.from_bytes(message.value()))
