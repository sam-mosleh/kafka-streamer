import io

import pytest
from pytest_mock import MockFixture

from kafka_streamer.models import SchematicSerializable, Serializable
from kafka_streamer.topic.datatype.base import KafkaDataType


@pytest.fixture
def kafka_data_type(mocker: MockFixture):
    return mocker.patch.multiple(KafkaDataType, __abstractmethods__=set())


@pytest.fixture
def register_schema(mocker: MockFixture):
    return mocker.patch.object(KafkaDataType, "register_schema", autospec=True)


def test_topic_magic_bytes():
    assert KafkaDataType._MAGIC_BYTE == 0


def test_kafka_data_type_autoregister(register_schema, kafka_data_type):
    KafkaDataType(SchematicSerializable, auto_register_schema=False)
    register_schema.assert_not_called()
    KafkaDataType(SchematicSerializable, auto_register_schema=True)
    register_schema.assert_called_once()


def test_kafka_data_type_register_schema_without_topic_or_registry(
    kafka_data_type):
    with pytest.raises(RuntimeError):
        KafkaDataType(SchematicSerializable)
    with pytest.raises(RuntimeError):
        KafkaDataType(SchematicSerializable, topic="test-topic")


def test_kafka_data_type_registering(mocker: MockFixture, kafka_data_type):
    registry = mocker.MagicMock()
    registry.register_schema.return_value = 10
    mocker.patch.object(
        SchematicSerializable,
        "get_model_schema",
        return_value="schema string",
        autospec=True,
    )
    mocker.patch.object(KafkaDataType,
                        "get_subject",
                        return_value="subject",
                        autospec=True)
    data_type = KafkaDataType(SchematicSerializable,
                              topic="test-topic",
                              schema_registry=registry)
    registry.register_schema.assert_called_once_with("subject",
                                                     "schema string")
    assert data_type.schema_id == 10


def test_deserialize_bytes_type(kafka_data_type):
    data_type = KafkaDataType(bytes)
    assert b"1234" == data_type.deserialize(b"1234")


def test_deserialize_serializable_type(mocker: MockFixture, kafka_data_type):
    data_type = KafkaDataType(Serializable)
    from_bytes = mocker.patch.object(Serializable,
                                     "from_bytes",
                                     return_value=b"5678",
                                     autospec=True)
    assert data_type.deserialize(b"1234") == b"5678"
    from_bytes.assert_called_once_with(b"1234")


def test_deserialize_schema_with_invalid_magic_byte(mocker: MockFixture,
                                                    kafka_data_type):
    from_bytes = mocker.patch.object(SchematicSerializable,
                                     "from_bytes",
                                     autpspec=True)
    data_type = KafkaDataType(SchematicSerializable,
                              auto_register_schema=False)
    with pytest.raises(TypeError):
        data_type._deserialize_schema(io.BytesIO(b"\x01\x02\x03\x04\x05"),
                                      SchematicSerializable)


def test_deserialize_schema(mocker: MockFixture, kafka_data_type):
    from_bytes = mocker.patch.object(SchematicSerializable,
                                     "from_bytes",
                                     autpspec=True)
    register = mocker.MagicMock()
    register.schema_id_size = 4
    register.get_schema.return_value = "schema"
    data_type = KafkaDataType(SchematicSerializable,
                              "test-topic",
                              register,
                              auto_register_schema=False)
    bio = io.BytesIO(b"\x00\x01\x02\x03\x04")
    data_type._deserialize_schema(bio)
    from_bytes.assert_called_once_with(bio, 16909060, "schema")


# @pytest.mark.skip
# def test_deserialize_schemaserializable_type(mocker: MockFixture):
#     topic = BaseTopic("test-topic")
#     deserialize_schema = mocker.patch.object(
#         BaseTopic, "_deserialize_schema", autospec=True
#     )
#     bio = io.BytesIO(b"1234")
#     bio_mock = mocker.patch(
#         "kafka_streamer.topic.base.io.BytesIO", return_value=bio, autospec=True
#     )
#     topic.deserialize(b"1234", SchematicSerializable)
#     bio_mock.assert_called_once_with(b"1234")
#     deserialize_schema.assert_called_once_with(topic, bio, SchematicSerializable)
