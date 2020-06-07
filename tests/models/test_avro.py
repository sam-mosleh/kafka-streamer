import io
import json

import fastavro
import pytest

from kafka_streamer.exceptions import MessageDeserializationError
from kafka_streamer.models import AvroRecord


class SimpleModel(AvroRecord):
    x: int
    y: str
    pi: float = 3.14


simple_model_schema = {
    "namespace": "base",
    "name": "SimpleModel",
    "type": "record",
    "fields": [
        {"name": "x", "type": "long"},
        {"name": "y", "type": "string"},
        {"name": "pi", "type": "double", "default": 3.14},
    ],
}

parsed_schema = fastavro.parse_schema(simple_model_schema)


def test_simple_avro_model_schema():
    assert SimpleModel.get_model_schema() == json.dumps(simple_model_schema)


def test_serialized_simple_avro_model():
    model = SimpleModel(x=10, y="Ten")
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, parsed_schema, {"x": 10, "y": "Ten", "pi": 3.14})
    assert bio.getvalue() == model.to_bytes()


def test_not_to_change_after_serialization_deserialization():
    model = SimpleModel(x=30, y="Thirty")
    bio = io.BytesIO(model.to_bytes())
    assert model == SimpleModel.from_bytes(bio, 0, json.dumps(simple_model_schema))


def test_deserializing_simple_avro_model():
    bio = io.BytesIO()
    fastavro.schemaless_writer(
        bio, parsed_schema, {"x": 20, "y": "Twenty", "pi": 3.1415}
    )
    bio.seek(0)
    model = SimpleModel.from_bytes(bio, 0, json.dumps(simple_model_schema))
    assert model.x == 20
    assert model.y == "Twenty"
    assert model.pi == 3.1415


extended_model_schema = {
    "type": "record",
    "name": "ExtendedSchema",
    "fields": [
        {"name": "x", "type": "long"},
        {"name": "y", "type": "string"},
        {"default": 3.14, "name": "pi", "type": "double"},
        {"default": 10, "name": "z", "type": "long"},
    ],
}


def test_extended_schema_compatibility():
    parsed_extended = fastavro.parse_schema(extended_model_schema)
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, parsed_extended, {"x": 50, "y": "Fifty", "z": 100})
    bio.seek(0)
    model = SimpleModel.from_bytes(bio, 0, json.dumps(extended_model_schema))
    assert model.x == 50
    assert model.y == "Fifty"
    assert model.pi == 3.14


incompatible_schema = {
    "namespace": "base",
    "name": "SimpleModel",
    "type": "record",
    "fields": [{"name": "x", "type": "long"},],
}


def test_incompatible_schema():
    parsed_incompatible_model = fastavro.parse_schema(incompatible_schema)
    bio = io.BytesIO()
    fastavro.schemaless_writer(bio, parsed_incompatible_model, {"x": 10})
    bio.seek(0)
    with pytest.raises(MessageDeserializationError):
        SimpleModel.from_bytes(bio, 0, json.dumps(incompatible_schema))
