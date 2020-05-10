import asyncio
import collections
import functools
import inspect
import logging
import re
from typing import Callable, List

import confluent_kafka

from .consumer import KafkaConsumer
from .producer import KafkaProducer


def async_wrap(func):
    if asyncio.iscoroutinefunction(func):
        return func

    @functools.wraps(func)
    async def run(*args, **kwargs):
        loop = asyncio.get_running_loop()
        pfunc = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)

    return run


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
            func_async = async_wrap(func)
            params = inspect.signature(func_async).parameters
            if len(params) > 1:
                raise TypeError(
                    'Topic consumers must have only one parameter.')
            first_param_annotation = params[next(iter(params))].annotation
            if first_param_annotation == inspect._empty:
                first_param_annotation = None
            func_async.convertor = first_param_annotation
            self._add_callback_to_selector(func_async, topic_selector)
            return func_async

        return decorator

    def _add_callback_to_selector(self, func, topic_selector):
        if topic_selector.startswith('^'):
            pattern = re.compile(topic_selector)
            self._regex_selectors[pattern].append(func)
        else:
            self._simple_selectors[topic_selector].append(func)

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
                break

    async def _call_message_handlers(self, msg: confluent_kafka.Message):
        tasks = []
        for consumer_callback in self._get_topic_consumers(msg.topic()):
            if consumer_callback.convertor == bytes:
                task = consumer_callback(msg.value())
            elif consumer_callback.convertor is None or \
                 consumer_callback.convertor == confluent_kafka.Message:
                task = consumer_callback(msg)
            else:
                model = consumer_callback.convertor.from_bytes(msg.value())
                task = consumer_callback(model)
            tasks.append(task)
        return [
            callback_result for callback_result in await asyncio.gather(*tasks)
            if callback_result is not None
        ]

    def _get_topic_consumers(self, topic_name: str) -> List[Callable]:
        consumers = []
        if topic_name in self._simple_selectors:
            consumers.extend(self._simple_selectors[topic_name])
        for pattern in self._regex_selectors:
            if pattern.match(topic_name):
                consumers.extend(self._regex_selectors[pattern])
        return consumers
