import asyncio
from typing import Optional
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture

from kafka_streamer.client import AsyncKafkaConsumer

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def consumer() -> AsyncKafkaConsumer:
    async with AsyncKafkaConsumer([], 'test-group', ['test-topic']) as c:
        yield c


async def test_subscription_after_enter(mocker: MockFixture, consumer):
    subscribe = mocker.patch(
        'kafka_streamer.client.consumer.AsyncKafkaConsumer.subscribe',
        autospec=True)
    async with AsyncKafkaConsumer([], 'test-group', ['test-topic']) as c:
        subscribe.assert_called_once_with(c, ['test-topic'])


async def test_close_consumer_after_exit(mocker: MockFixture):
    close = mocker.patch(
        'kafka_streamer.client.consumer.AsyncKafkaConsumer.close',
        autospec=True)
    async with AsyncKafkaConsumer([], 'test-group', ['test-topic']) as c:
        pass
    close.assert_called_once_with(c)


class SampleMessage:
    def __init__(self, error: str = ''):
        self._error = error

    def error(self):
        return self._error


def test_message_validator():
    c = AsyncKafkaConsumer([], '')
    assert c._message_validator(None) is False
    assert c._message_validator(SampleMessage('Some Error')) is False
    assert c._message_validator(SampleMessage()) is True


async def test_consumer_fetch(mocker: MockFixture,
                              consumer: AsyncKafkaConsumer):
    message = SampleMessage()
    poll = mocker.patch(
        'kafka_streamer.client.consumer.AsyncKafkaConsumer.poll',
        autospec=True)
    poll.return_value = message
    assert await consumer.fetch() == message


async def test_kafka_to_queue(mocker: MockFixture):
    message = SampleMessage()
    aenter = mocker.patch(
        'kafka_streamer.client.consumer.AsyncKafkaConsumer.__aenter__',
        autospec=True)
    aexit = mocker.patch(
        'kafka_streamer.client.consumer.AsyncKafkaConsumer.__aexit__',
        return_value=False,
        autospec=True)
    mocker.patch('kafka_streamer.client.consumer.AsyncKafkaConsumer.fetch',
                 side_effect=[message, RuntimeError()],
                 autospec=True)
    put = mocker.patch.object(asyncio.Queue, 'put', autospec=True)
    q = asyncio.Queue()
    c = AsyncKafkaConsumer([], '')
    with pytest.raises(RuntimeError):
        await c.kafka_to_queue(q)
    aenter.assert_called()
    put.assert_called_once_with(q, message)
    aexit.assert_called()
