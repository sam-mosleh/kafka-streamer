from kafka_streamer.topic.datatype import KafkaKey


def test_kafka_key_subject():
    key = KafkaKey(bytes, "test-topic")
    assert key.get_subject() == "test-topic-key"
