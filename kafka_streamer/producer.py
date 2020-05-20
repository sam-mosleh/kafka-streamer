from __future__ import annotations

import asyncio
import logging
from typing import Optional

import confluent_kafka

from . import utils


class AsyncKafkaProducer:
    def __init__(self,
                 hosts: str,
                 max_flush_time_on_full_buffer: float = 5.0,
                 logger: logging.Logger = None,
                 debug: bool = False):
        conf = {
            'bootstrap.servers': hosts,
            'error_cb': self.error_callback,
            'stats_cb': self.stats_callback,
            'throttle_cb': self.throttle_callback,
            'on_delivery': self.delivery_report_callback
        }
        if debug:
            conf['debug'] = 'topic,broker'

        self.max_flush_time_on_full_buffer = max_flush_time_on_full_buffer
        self.logger = logger if logger is not None else logging.getLogger(
            'KafkaProducer')
        self._kafka_instance = confluent_kafka.Producer(conf,
                                                        logger=self.logger)

    def error_callback(self, error: confluent_kafka.KafkaError):
        pass

    def stats_callback(self, json_str: str):
        pass

    def throttle_callback(self, event: confluent_kafka.ThrottleEvent):
        pass

    def delivery_report_callback(self,
                                 err: Optional[confluent_kafka.KafkaError],
                                 msg: confluent_kafka.Message):
        if err is not None:
            self.logger.warning(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to => "
                              f"Topic: {msg.topic()} "
                              f"Partition: {msg.partition()}")

    async def __aenter__(self) -> AsyncKafkaProducer:
        self._poller_task = asyncio.create_task(self.poll_forever())
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if not self._poller_task.cancelled():
            self._poller_task.cancel()
        await self.flush_until_all_messages_are_sent()

    async def poll(self, timeout: float = None):
        return await utils.call_sync_function_without_none_parameter(
            self._kafka_instance.poll, timeout=timeout)

    async def flush(self, timeout: float = None):
        return await utils.call_sync_function_without_none_parameter(
            self._kafka_instance.flush, timeout=timeout)

    async def produce(self, topic: str, value: bytes, key: bytes = None):
        try:
            self._kafka_instance.poll(0)
            self._kafka_instance.produce(topic=topic, value=value, key=key)
        except BufferError as bf:
            self.logger.warning(f"Buffer error : {bf}")
            await self.flush(self.max_flush_time_on_full_buffer)
            self._kafka_instance.produce(topic=topic, value=value, key=key)

    async def queue_to_kafka(self, queue: asyncio.Queue):
        async with self:
            try:
                while item := await queue.get():
                    await self.produce(topic=item['topic'],
                                       value=item['value'],
                                       key=item['key'])
            except asyncio.CancelledError:
                await self._produce_remaining_items_in_queue(queue)
                raise

    async def _produce_remaining_items_in_queue(self, queue: asyncio.Queue):
        items_in_queue = queue.qsize()
        if items_in_queue > 0:
            self.logger.warning(f"There are {items_in_queue} items"
                                " in producer queue. Waiting to produce them.")
            while queue.empty() is False:
                item = await queue.get()
                await self.produce(topic=item['topic'],
                                   value=item['value'],
                                   key=item['key'])
            self.logger.info("Producer queue is empty now")

    async def poll_forever(self):
        while True:
            await self.poll(1.0)
            await asyncio.sleep(1.0)

    async def flush_until_all_messages_are_sent(self):
        self.logger.info('Flushing')
        while True:
            pending_messages = await self.flush(1.0)
            self.logger.info(f"Flushing> Pending: {pending_messages} messages")
            if pending_messages == 0:
                break
        self.logger.info('Flushing state finished')
