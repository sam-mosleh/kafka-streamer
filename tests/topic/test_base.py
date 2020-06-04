import io
import struct
from unittest.mock import AsyncMock

import pytest
from pytest_mock import MockFixture

from kafka_streamer.models import SchematicSerializable, Serializable
from kafka_streamer.topic import BaseTopic
from tests.message import SampleMessage
from tests.registry import SampleRegistry


def test_topic_magic_bytes():
    topic = BaseTopic("test-topic")
    assert topic._MAGIC_BYTE == 0


def test_topic_consumer_with_invalid_parameter_name():
    topic = BaseTopic("test-topic")
    with pytest.raises(TypeError):

        @topic
        def f(invalid_parameter):
            pass


def test_topic_consumer_without_parameter():
    topic = BaseTopic("test-topic")
    with pytest.raises(TypeError):

        @topic
        def f():
            pass


def test_topic_decorator_adds_consumer(mocker: MockFixture):
    add = mocker.patch.object(BaseTopic, "_add", autospec=True)
    topic = BaseTopic("test-topic")
    add.assert_not_called()

    @topic
    async def f(value: bytes):
        pass

    print("topic:", f)

    add.assert_called_once_with(topic, f, {"value"})


def test_topic_consumers_check():
    topic = BaseTopic("test-topic")
    assert topic.has_consumer() is False

    @topic
    def f(value: bytes):
        pass

    assert topic.has_consumer() is True


@pytest.mark.asyncio
async def test_multiple_message_handlers():
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
async def test_consumer_with_key_parameter():
    msg = SampleMessage(value=b"1234", key=b"5678")
    topic = BaseTopic("test-topic")
    f = AsyncMock()
    topic._add(f, {"key"})
    await topic.message_handlers(msg)
    f.assert_called_once_with(key=b"5678")


@pytest.mark.asyncio
async def test_consumer_with_key_value_parameter():
    msg = SampleMessage(value=b"1234", key=b"5678")
    topic = BaseTopic("test-topic")
    f = AsyncMock()
    topic._add(f, {"key", "value"})
    await topic.message_handlers(msg)
    f.assert_called_once_with(value=b"1234", key=b"5678")


@pytest.mark.asyncio
async def test_consumer_with_offset_parameter():
    msg = SampleMessage(value=b"1234", offset=100)
    topic = BaseTopic("test-topic")
    f = AsyncMock()
    topic._add(f, {"offset"})
    await topic.message_handlers(msg)
    f.assert_called_once_with(offset=100)


def test_deserialize_nothing():
    topic = BaseTopic("test-topic")
    assert b"1234" == topic.deserialize(b"1234", bytes)


def test_deserialize_serializable(mocker: MockFixture):
    topic = BaseTopic("test-topic")
    from_bytes = mocker.patch.object(Serializable, "from_bytes", autospec=True)
    topic.deserialize(b"1234", Serializable)
    from_bytes.assert_called_once_with(b"1234")


def test_deserialize_schema(mocker: MockFixture):
    registry = SampleRegistry()
    topic = BaseTopic("test-topic", schema_registry=registry)
    from_bytes = mocker.patch.object(SchematicSerializable, "from_bytes", autpspec=True)
    with pytest.raises(TypeError):
        topic._deserialize_schema(io.BytesIO(b"12345"), SchematicSerializable)
    bio = io.BytesIO(b"\x00\x01\x02\x03\x04")
    topic._deserialize_schema(bio, SchematicSerializable)
    from_bytes.assert_called_once_with(
        bio, struct.unpack(">I", b"\x01\x02\x03\x04")[0], ""
    )
