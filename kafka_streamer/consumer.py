from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

import confluent_kafka

from . import utils


class AsyncKafkaConsumer:
    def __init__(self,
                 hosts: str,
                 group_id: str,
                 subscription: List[str] = [],
                 auto_offset: bool = True,
                 logger: logging.Logger = None,
                 debug: bool = False):
        conf = {
            'bootstrap.servers': hosts,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.offset.store': auto_offset,
            'statistics.interval.ms': 1000,
            'error_cb': self.error_callback,
            'stats_cb': self.stats_callback,
            'throttle_cb': self.throttle_callback
        }
        if debug:
            conf['debug'] = 'consumer'
        self.subscription = subscription
        self.logger = logger if logger is not None else logging.getLogger(
            'KafkaConsumer')
        self._kafka_instance = confluent_kafka.Consumer(conf,
                                                        logger=self.logger)

    def error_callback(self, error: confluent_kafka.KafkaError):
        pass

    def stats_callback(self, json_str: str):
        pass

    def throttle_callback(self, event: confluent_kafka.ThrottleEvent):
        pass

    def assign_callback(self, consumer, partitions):
        pass

    def revoke_callback(self, consumer, partitions):
        pass

    async def __aenter__(self) -> AsyncKafkaConsumer:
        self.send_subscription_to_kafka()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close_instance()

    def send_subscription_to_kafka(self):
        self._kafka_instance.subscribe(self.subscription,
                                       on_assign=self.assign_callback,
                                       on_revoke=self.revoke_callback)
        self.logger.info(f"Subscribed to {self.subscription}")

    def close_instance(self):
        self.logger.info('Closing consumer.')
        self._kafka_instance.close()

    async def poll(self,
                   timeout: float = None) -> Optional[confluent_kafka.Message]:
        return await utils.call_sync_function_without_none_parameter(
            self._kafka_instance.poll, timeout=timeout)

    async def kafka_to_queue(self, queue: asyncio.Queue):
        async with self:
            while True:
                message = await self.fetch()
                await queue.put(message)

    async def fetch(self) -> confluent_kafka.Message:
        while True:
            message = self._kafka_instance.poll(0) or await self.poll(1.0)
            if message is not None:
                if message.error():
                    self.logger.error(message.error())
                    continue
                return message

    def set_offset(self, message: confluent_kafka.Message):
        self._kafka_instance.store_offsets(message)
