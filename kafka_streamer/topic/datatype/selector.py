from typing import Optional, Type, Union

from confluent_avro.schema_registry import SchemaRegistry

from kafka_streamer.models import SchematicRecord, Serializable

from .byte import ByteDataType
from .schematic import SchematicDataType
from .serializable import SerializableDataType


def datatype_selector(
    datatype: Union[SchematicRecord, Type[Serializable], Type[bytes]],
    topic: str = "",
    subject_postfix: str = "",
    schema_registry: Optional[SchemaRegistry] = None,
) -> Union[ByteDataType, SerializableDataType, SchematicDataType]:
    if datatype == bytes:
        return ByteDataType()
    elif isinstance(datatype, SchematicRecord):
        return SchematicDataType(
            datatype,
            topic=topic,
            schema_registry=schema_registry,
            subject_postfix=subject_postfix,
        )
    elif issubclass(datatype, Serializable):
        return SerializableDataType(datatype)


def keytype_selector(
    datatype: Union[SchematicRecord, Type[Serializable], Type[bytes]],
    topic: str = "",
    subject_postfix: str = "",
    schema_registry: Optional[SchemaRegistry] = None,
) -> Union[ByteDataType, SerializableDataType, SchematicDataType]:
    return datatype_selector(
        datatype, topic=topic, subject_postfix="key", schema_registry=schema_registry,
    )


def valuetype_selector(
    datatype: Union[SchematicRecord, Type[Serializable], Type[bytes]],
    topic: str = "",
    subject_postfix: str = "",
    schema_registry: Optional[SchemaRegistry] = None,
) -> Union[ByteDataType, SerializableDataType, SchematicDataType]:
    return datatype_selector(
        datatype, topic=topic, subject_postfix="value", schema_registry=schema_registry,
    )
