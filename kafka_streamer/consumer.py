import asyncio
import logging
from typing import List, Optional

import confluent_kafka


class KafkaConsumer:
    def __init__(self,
                 hosts: str,
                 group_id: str,
                 subscription: List[str] = [],
                 logger: logging.Logger = None):
        self.conf = {
            'bootstrap.servers': hosts,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'statistics.interval.ms': 1000,
            'error_cb': self.error_callback,
            'stats_cb': self.stats_callback,
            'throttle_cb': self.throttle_callback
        }
        self.subscription = subscription
        self.logger = logger if logger is not None else logging.getLogger(
            'KafkaConsumer')
        self._kafka_instance = confluent_kafka.Consumer(self.conf)

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

    async def __aenter__(self):
        self.send_subscription_to_kafka()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close_instance()
        return self

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
        loop = asyncio.get_running_loop()
        if timeout is not None:
            future = loop.run_in_executor(None, self._kafka_instance.poll,
                                          timeout)
        else:
            future = loop.run_in_executor(None, self._kafka_instance.poll)
        return await future

    async def kafka_to_queue(self, queue: asyncio.Queue):
        async with self:
            while True:
                message = await self.fetch()
                await queue.put(message)

    async def fetch(self) -> confluent_kafka.Message:
        while True:
            message = await self.poll(1.0)
            if message is not None:
                if message.error():
                    self.logger.error(message.error())
                    continue
                return message
