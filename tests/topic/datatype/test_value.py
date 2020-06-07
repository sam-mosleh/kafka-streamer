from kafka_streamer.topic.datatype import KafkaValue


def test_kafka_value_subject():
    key = KafkaValue(bytes, "test-topic")
    assert key.get_subject() == "test-topic-value"
