import asyncio
import logging
from typing import Optional

import confluent_kafka


class KafkaProducer:
    def __init__(self,
                 hosts: str,
                 max_flush_time_on_full_buffer: float = 5.0,
                 logger: logging.Logger = None):
        self.max_flush_time_on_full_buffer = max_flush_time_on_full_buffer
        self.logger = logger if logger is not None else logging.getLogger(
            'KafkaProducer')
        self.conf = {
            'bootstrap.servers': hosts,
            'error_cb': self.error_callback,
            'stats_cb': self.stats_callback,
            'throttle_cb': self.throttle_callback
        }
        self._kafka_instance = confluent_kafka.Producer(self.conf)

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

    async def __aenter__(self):
        self._poller_task = asyncio.create_task(self.poll_forever())
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._poller_task.cancel()
        self.logger.info('Flushing')
        while True:
            pending_messages = await self.flush(1.0)
            # future = self.flush(1.0)
            # pending_messages = await future
            # try:
            #     pending_messages = await future
            # except asyncio.CancelledError:
            #     pending_messages = await future
            self.logger.info(f"Flushing> Pending: {pending_messages} messages")
            if pending_messages == 0:
                break
        self.logger.info('Flushing state finished')
        return self

    async def poll(self, timeout: float = None):
        loop = asyncio.get_running_loop()
        if timeout is not None:
            future = loop.run_in_executor(None, self._kafka_instance.poll,
                                          timeout)
        else:
            future = loop.run_in_executor(None, self._kafka_instance.poll)
        return await future

    async def flush(self, timeout: float = None):
        print('IN FLUSH')
        loop = asyncio.get_running_loop()
        if timeout is not None:
            future = loop.run_in_executor(None, self._kafka_instance.flush,
                                          timeout)
        else:
            future = loop.run_in_executor(None, self._kafka_instance.flush)
        print('Returning from flush')
        return await future

    async def produce(self, topic, key: bytes = None, value: bytes = None):
        try:
            self._kafka_instance.poll(0)
            self._kafka_instance.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=self.delivery_report_callback)
        except BufferError as bf:
            self.logger.warning(f"Buffer error : {bf}")
            await self.flush(self.max_flush_time_on_full_buffer)
            self._kafka_instance.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=self.delivery_report_callback)

    async def queue_to_kafka(self, queue: asyncio.Queue):
        async with self:
            try:
                while item := await queue.get():
                    await self.produce(item.topic, item.key, item.value)
            except asyncio.CancelledError:
                items_in_queue = queue.qsize()
                if items_in_queue > 0:
                    self.logger.warning(
                        f"There are {items_in_queue} items"
                        " in producer queue. Waiting to produce them.")
                    while queue.empty() is False:
                        item = await queue.get()
                        await self.produce(item.topic, item.key, item.value)
                    self.logger.info("Producer queue is empty now")
                raise asyncio.CancelledError()

    async def poll_forever(self):
        while True:
            await self.poll(1.0)
            await asyncio.sleep(1.0)
