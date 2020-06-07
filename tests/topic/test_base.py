import io
import struct
from unittest.mock import AsyncMock

import pytest
from pytest_mock import MockFixture

from kafka_streamer.models import SchematicSerializable, Serializable
from kafka_streamer.topic import BaseTopic
from tests.message import SampleMessage
from tests.registry import SampleRegistry


@pytest.fixture
def create_value(mocker):
    return mocker.patch.object(BaseTopic, "create_value", autospec=True)


@pytest.fixture
def create_key(mocker):
    return mocker.patch.object(BaseTopic, "create_key", autospec=True)


def test_key_value_created(create_value, create_key):
    topic = BaseTopic("test-topic")
    create_value.assert_called_once()
    create_key.assert_called_once()


def test_topic_consumer_with_invalid_parameter_name(create_value, create_key):
    topic = BaseTopic("test-topic")
    with pytest.raises(TypeError):

        @topic
        def f(invalid_parameter):
            pass


def test_topic_consumer_without_parameter(create_value, create_key):
    topic = BaseTopic("test-topic")
    with pytest.raises(TypeError):

        @topic
        def f():
            pass


def test_topic_decorator_adds_consumer(mocker: MockFixture, create_value, create_key):
    add = mocker.patch.object(BaseTopic, "_add", autospec=True)
    topic = BaseTopic("test-topic")
    add.assert_not_called()

    @topic
    async def f(value: bytes):
        pass

    add.assert_called_once_with(topic, f, {"value"})


def test_topic_consumers_check(create_value, create_key):
    topic = BaseTopic("test-topic")
    assert topic.has_consumer() is False

    @topic
    def f(value: bytes):
        pass

    assert topic.has_consumer() is True


@pytest.mark.asyncio
async def test_multiple_message_handlers(mocker: MockFixture, create_value, create_key):
    kafka_data_type_mock = mocker.MagicMock()
    kafka_data_type_mock.deserialize.return_value = b"1234"
    create_value.return_value = kafka_data_type_mock
    msg = SampleMessage(value=b"1234")
    topic = BaseTopic("test-topic")
    mock_one = AsyncMock()
    mock_two = AsyncMock()
    topic._add(mock_one, {"value"})
    topic._add(mock_two, {"value"})
    await topic.message_handlers(msg)
    mock_one.assert_called_once_with(value=b"1234")
    mock_two.assert_called_once_with(value=b"1234")


@pytest.mark.asyncio
async def test_consumer_with_key_parameter(
    mocker: MockFixture, create_value, create_key
):
    kafka_data_type_mock = mocker.MagicMock()
    kafka_data_type_mock.deserialize.return_value = b"5678"
    create_key.return_value = kafka_data_type_mock
    msg = SampleMessage(value=b"1234", key=b"5678")
    topic = BaseTopic("test-topic")
    f = AsyncMock()
    topic._add(f, {"key"})
    await topic.message_handlers(msg)
    f.assert_called_once_with(key=b"5678")


@pytest.mark.asyncio
async def test_consumer_with_key_value_parameter(
    mocker: MockFixture, create_value, create_key
):
    kafka_data_type_mock = mocker.MagicMock()
    kafka_data_type_mock.deserialize.return_value = b"1111"
    create_value.return_value = kafka_data_type_mock
    create_key.return_value = kafka_data_type_mock
    msg = SampleMessage(value=b"1111", key=b"1111")
    topic = BaseTopic("test-topic")
    f = AsyncMock()
    topic._add(f, {"key", "value"})
    await topic.message_handlers(msg)
    f.assert_called_once_with(value=b"1111", key=b"1111")


@pytest.mark.asyncio
async def test_consumer_with_offset_parameter(create_value, create_key):
    msg = SampleMessage(value=b"1234", offset=100)
    topic = BaseTopic("test-topic")
    f = AsyncMock()
    topic._add(f, {"offset"})
    await topic.message_handlers(msg)
    f.assert_called_once_with(offset=100)
