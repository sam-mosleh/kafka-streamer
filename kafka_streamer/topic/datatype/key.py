from .base import KafkaDataType


class KafkaKey(KafkaDataType):
    subject_postfix: str = "key"
