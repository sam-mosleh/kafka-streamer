from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

import confluent_kafka

from kafka_streamer.utils import async_wrap


class AsyncKafkaConsumer:
    def __init__(
        self,
        hosts: List[str],
        group_id: str,
        subscription: List[str] = [],
        auto_offset: bool = True,
        start_from_beginning_if_no_offset_available: bool = True,
        statistics_interval_ms: int = 1000,
        use_confluent_monitoring_interceptor: bool = False,
        logger: Optional[logging.Logger] = None,
        debug: bool = False,
    ):
        conf = {
            "bootstrap.servers": ",".join(hosts),
            "group.id": group_id,
            "enable.auto.offset.store": auto_offset,
            "statistics.interval.ms": statistics_interval_ms,
            "error_cb": self.error_callback,
            "stats_cb": self.stats_callback,
            "throttle_cb": self.throttle_callback,
        }
        if start_from_beginning_if_no_offset_available:
            conf["auto.offset.reset"] = "earliest"
        if use_confluent_monitoring_interceptor:
            conf["plugin.library.paths"] = "monitoring-interceptor"
        if debug:
            conf["debug"] = "consumer"
        self.subscription = subscription
        self.logger = logger or logging.getLogger("KafkaConsumer")
        self._kafka_instance = confluent_kafka.Consumer(conf, logger=self.logger)
        self._async_poll = async_wrap(self._kafka_instance.poll)

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
        if len(self.subscription) > 0:
            self.subscribe(self.subscription)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.close()

    def subscribe(self, subscription: List[str]):
        self._kafka_instance.subscribe(
            subscription,
            on_assign=self.assign_callback,
            on_revoke=self.revoke_callback,
        )
        self.logger.info(f"Subscription to {self.subscription} sent")

    def close(self):
        self._kafka_instance.close()
        self.logger.info("Consumer closed.")

    async def poll(self, timeout: float = 1.0) -> Optional[confluent_kafka.Message]:
        return self._kafka_instance.poll(0) or await self._async_poll(timeout=timeout)

    def set_offset(self, message: confluent_kafka.Message):
        self._kafka_instance.store_offsets(message)

    async def kafka_to_queue(self, queue: asyncio.Queue):
        async with self:
            while True:
                message = await self.fetch()
                await queue.put(message)

    async def fetch(self) -> confluent_kafka.Message:
        while True:
            message = await self.poll()
            if self._message_validator(message) is True:
                return message

    def _message_validator(self, message: Optional[confluent_kafka]) -> bool:
        if message is None:
            return False
        if message.error():
            self.logger.error(message.error())
            return False
        else:
            return True
