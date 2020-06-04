import asyncio

import pytest
from pytest_mock import MockFixture

from kafka_streamer.client import AsyncKafkaConsumer
from tests.message import SampleMessage

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def consumer() -> AsyncKafkaConsumer:
    return AsyncKafkaConsumer([], "test-group", ["test-topic"])


@pytest.fixture
def aenter(mocker):
    return mocker.patch(
        "kafka_streamer.client.consumer.AsyncKafkaConsumer.__aenter__", autospec=True
    )


@pytest.fixture
def aexit(mocker):
    return mocker.patch(
        "kafka_streamer.client.consumer.AsyncKafkaConsumer.__aexit__",
        return_value=False,
        autospec=True,
    )


async def test_subscription_after_enter(
    mocker: MockFixture, consumer: AsyncKafkaConsumer, aexit
):
    subscribe = mocker.patch(
        "kafka_streamer.client.consumer.AsyncKafkaConsumer.subscribe", autospec=True
    )
    async with consumer:
        subscribe.assert_called_once_with(consumer, ["test-topic"])


async def test_close_consumer_after_exit(
    mocker: MockFixture, consumer: AsyncKafkaConsumer
):
    close = mocker.patch(
        "kafka_streamer.client.consumer.AsyncKafkaConsumer.close", autospec=True
    )
    async with consumer:
        pass
    close.assert_called_once_with(consumer)


def test_message_validator():
    c = AsyncKafkaConsumer([], "")
    assert c._message_validator(None) is False
    assert c._message_validator(SampleMessage(error="Some Error")) is False
    assert c._message_validator(SampleMessage()) is True


async def test_consumer_fetch(mocker: MockFixture, consumer: AsyncKafkaConsumer):
    message = SampleMessage()
    mocker.patch(
        "kafka_streamer.client.consumer.AsyncKafkaConsumer.poll",
        return_value=message,
        autospec=True,
    )
    assert await consumer.fetch() == message


async def test_kafka_to_queue(
    mocker: MockFixture, consumer: AsyncKafkaConsumer, aenter, aexit
):
    message = SampleMessage()
    mocker.patch(
        "kafka_streamer.client.consumer.AsyncKafkaConsumer.fetch",
        side_effect=[message, Exception("break while loop")],
        autospec=True,
    )
    put = mocker.patch.object(asyncio.Queue, "put", autospec=True)
    q = asyncio.Queue()
    with pytest.raises(Exception, match="break while loop"):
        await consumer.kafka_to_queue(q)
    aenter.assert_called()
    put.assert_called_once_with(q, message)
    aexit.assert_called()
