from __future__ import annotations

import asyncio
import logging
from typing import Optional, List

import confluent_kafka

from kafka_streamer.utils import async_wrap


class AsyncKafkaProducer:
    def __init__(
        self,
        hosts: List[str],
        max_flush_time_on_full_buffer: float = 5.0,
        statistics_interval_ms: int = 1000,
        logger: logging.Logger = None,
        debug: bool = False,
    ):
        conf = {
            "bootstrap.servers": ",".join(hosts),
            "statistics.interval.ms": statistics_interval_ms,
            "error_cb": self.error_callback,
            "stats_cb": self.stats_callback,
            "throttle_cb": self.throttle_callback,
            "on_delivery": self.delivery_report_callback,
        }
        if debug:
            conf["debug"] = "topic,broker"

        self.max_flush_time_on_full_buffer = max_flush_time_on_full_buffer
        self.logger = (
            logger if logger is not None else logging.getLogger("KafkaProducer")
        )
        self._kafka_instance = confluent_kafka.Producer(conf, logger=self.logger)
        self._async_poll = async_wrap(self._kafka_instance.poll)
        self._async_flush = async_wrap(self._kafka_instance.poll)

    def error_callback(self, error: confluent_kafka.KafkaError):
        pass

    def stats_callback(self, json_str: str):
        pass

    def throttle_callback(self, event: confluent_kafka.ThrottleEvent):
        pass

    def delivery_report_callback(
        self, err: Optional[confluent_kafka.KafkaError], msg: confluent_kafka.Message
    ):
        if err is not None:
            self.logger.warning(f"Message delivery failed: {err}")
        else:
            self.logger.debug(
                f"Message delivered to <"
                f"Topic: {msg.topic()}, "
                f"Partition: {msg.partition()}, "
                f"Offset: {msg.offset()}>"
            )

    async def __aenter__(self) -> AsyncKafkaProducer:
        self.create_poller()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.cancel_poller()
        await self.flush_until_all_messages_are_sent()

    def create_poller(self):
        self._poller_task = asyncio.create_task(self.poll_forever())

    def cancel_poller(self):
        if not self._poller_task.cancelled():
            self._poller_task.cancel()

    async def poll(self, timeout: float = 0.0):
        return await self._async_poll(timeout=timeout)

    async def flush(self, timeout: float = 0.0):
        return await self._async_flush(timeout=timeout)

    def _send(self, topic: str, value: Union[bytes, str], key: Union[bytes, str]):
        self._kafka_instance.poll(0)
        self._kafka_instance.produce(topic=topic, value=value, key=key)

    async def produce(self, topic: str, value: bytes, key: bytes = None):
        try:
            self._send(topic, value, key)
        except BufferError as bf:
            self.logger.warning(f"Buffer error : {bf}")
            await self.flush(self.max_flush_time_on_full_buffer)
            self._send(topic, value, key)

    async def queue_to_kafka(self, queue: asyncio.Queue):
        async with self:
            try:
                await self._get_from_queue_and_produce(queue)
            except asyncio.CancelledError:
                if queue.qsize() > 0:
                    self.logger.warning(
                        f"There are {queue.qsize()} items"
                        " in producer queue. Waiting to produce them."
                    )
                # Send termination signal
                await queue.put(None)
                await self._get_from_queue_and_produce(queue)
                raise

    async def _get_from_queue_and_produce(self, queue: asyncio.Queue):
        while item := await queue.get():
            await self.produce(
                topic=item["topic"], value=item["value"], key=item["key"]
            )
        if queue.qsize() > 0:
            self.logger.error(
                f"There are {queue.qsize()} items"
                " in producer queue. Discarding them."
            )
        self.logger.info("Producing from queue finished")

    async def poll_forever(self):
        while True:
            await self.poll(1.0)
            await asyncio.sleep(1.0)

    async def flush_until_all_messages_are_sent(self, flush_timeouts: float = 1.0):
        self.logger.info("Flushing")
        while True:
            pending_messages = await self.flush(flush_timeouts)
            self.logger.info(f"Flushing> Pending: {pending_messages} messages")
            if pending_messages == 0:
                break
        self.logger.info("Flushing state finished")
