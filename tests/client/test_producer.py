import asyncio

import pytest
from pytest_mock import MockFixture

from kafka_streamer.client import AsyncKafkaProducer

pytestmark = pytest.mark.asyncio


@pytest.fixture
def create_mock_coro(mocker, monkeypatch):
    """Create a mock-coro pair.
    The coro can be used to patch an async method while the mock can
    be used to assert calls to the mocked out method.
    """

    def _create_mock_coro_pair(to_patch=None):
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)

        if to_patch:
            monkeypatch.setattr(to_patch, _coro)

        return mock, _coro

    return _create_mock_coro_pair


@pytest.fixture
async def producer() -> AsyncKafkaProducer:
    return AsyncKafkaProducer([])


@pytest.fixture
def aenter(mocker):
    return mocker.patch.object(AsyncKafkaProducer, "__aenter__", autospec=True)


@pytest.fixture
def aexit(mocker):
    return mocker.patch.object(
        AsyncKafkaProducer, "__aexit__", return_value=False, autospec=True,
    )


@pytest.fixture
def flush(mocker):
    return mocker.patch.object(
        AsyncKafkaProducer, "flush", return_value=0, autospec=True,
    )


@pytest.fixture
def produce(mocker):
    return mocker.patch.object(
        AsyncKafkaProducer, "produce", return_value=0, autospec=True,
    )


async def test_create_poller_after_enter(
    mocker: MockFixture, producer: AsyncKafkaProducer, aexit
):
    create_poller = mocker.patch.object(
        AsyncKafkaProducer, "create_poller", autospec=True
    )
    create_poller.assert_not_called()
    async with producer:
        create_poller.assert_called_once_with(producer)


async def test_cancel_poller_and_flush_after_exit(
    mocker: MockFixture, create_mock_coro, producer: AsyncKafkaProducer, aenter
):
    cancel_poller = mocker.patch.object(
        AsyncKafkaProducer, "cancel_poller", autospec=True
    )
    flush_until_all_messages_are_sent = mocker.patch.object(
        AsyncKafkaProducer,
        "flush_until_all_messages_are_sent",
        return_value=0,
        autospec=True,
    )
    async with producer:
        cancel_poller.assert_not_called()
        flush_until_all_messages_are_sent.assert_not_called()
    cancel_poller.assert_called_once_with(producer)
    flush_until_all_messages_are_sent.assert_called_once_with(producer)


async def test_poller_task(
    mocker: MockFixture, create_mock_coro, producer: AsyncKafkaProducer
):
    mock_poll_forever, _ = create_mock_coro(
        "kafka_streamer.client.producer.AsyncKafkaProducer.poll_forever"
    )
    producer.create_poller()
    ret_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert len(ret_tasks) == 1
    mock_poll_forever.assert_not_called()
    await asyncio.gather(*ret_tasks)
    mock_poll_forever.assert_called_once_with(producer)


async def test_produce(mocker: MockFixture, producer: AsyncKafkaProducer, flush):
    send = mocker.patch.object(AsyncKafkaProducer, "_send", autospec=True,)
    await producer.produce("test-topic", b"1234", b"5678")
    send.assert_called_once_with(producer, "test-topic", b"1234", b"5678")
    flush.assert_not_called()


async def test_produce_with_full_buffer(
    mocker: MockFixture, producer: AsyncKafkaProducer, flush
):
    send = mocker.patch.object(
        AsyncKafkaProducer,
        "_send",
        side_effect=[BufferError("Buffer is full"), None],
        autospec=True,
    )
    await producer.produce("test-topic", b"345", b"678")
    send.assert_called_with(producer, "test-topic", b"345", b"678")
    flush.assert_called_once()
    send.assert_called_with(producer, "test-topic", b"345", b"678")


async def test_kafka_to_queue(
    mocker: MockFixture, producer: AsyncKafkaProducer, produce, aenter, aexit
):
    message = {"topic": "test-topic", "value": b"135", "key": b"579"}
    q = asyncio.Queue()
    await q.put(message)
    await q.put(None)
    await producer.queue_to_kafka(q)
    aenter.assert_called()
    produce.assert_called_once_with(producer, "test-topic", b"135", b"579")
    aexit.assert_called()


async def test_cancelled_kafka_to_queue(
    mocker: MockFixture, producer: AsyncKafkaProducer, produce, aenter, aexit
):
    aexit.return_value = True
    message = {"topic": "test-topic", "value": b"135", "key": b"579"}
    get = mocker.patch.object(
        asyncio.Queue,
        "get",
        side_effect=[asyncio.CancelledError, message, None],
        autospec=True,
    )
    put = mocker.patch.object(asyncio.Queue, "put", autospec=True)
    q = asyncio.Queue()
    await producer.queue_to_kafka(q)
    put.assert_called_once_with(q, None)
    produce.assert_called_once_with(producer, "test-topic", b"135", b"579")
