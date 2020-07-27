from .base import KafkaDataType
from .byte import ByteDataType
from .serializable import SerializableDataType
from .schematic import SchematicDataType
from typing import Union, Type, Optional
from kafka_streamer.models import SchematicRecord, Serializable
from confluent_avro.schema_registry import SchemaRegistry


def datatype_selector(
    datatype: Union[SchematicRecord, Type[Serializable], Type[bytes]],
    topic: str = "",
    subject_postfix: str = "",
    schema_registry: Optional[SchemaRegistry] = None,
    auto_register_schema: bool = True,
) -> KafkaDataType:
    if datatype == bytes:
        return ByteDataType()
    elif isinstance(datatype, SchematicRecord):
        return SchematicDataType(
            datatype,
            topic=topic,
            schema_registry=schema_registry,
            auto_register_schema=auto_register_schema,
            subject_postfix=subject_postfix,
        )
    elif issubclass(datatype, Serializable):
        return SerializableDataType(datatype)


def keytype_selector(
    datatype: Union[SchematicRecord, Type[Serializable], Type[bytes]],
    topic: str = "",
    subject_postfix: str = "",
    schema_registry: Optional[SchemaRegistry] = None,
    auto_register_schema: bool = True,
) -> KafkaDataType:
    return datatype_selector(
        datatype,
        topic=topic,
        subject_postfix="key",
        schema_registry=schema_registry,
        auto_register_schema=auto_register_schema,
    )


def valuetype_selector(
    datatype: Union[SchematicRecord, Type[Serializable], Type[bytes]],
    topic: str = "",
    subject_postfix: str = "",
    schema_registry: Optional[SchemaRegistry] = None,
    auto_register_schema: bool = True,
) -> KafkaDataType:
    return datatype_selector(
        datatype,
        topic=topic,
        subject_postfix="value",
        schema_registry=schema_registry,
        auto_register_schema=auto_register_schema,
    )
