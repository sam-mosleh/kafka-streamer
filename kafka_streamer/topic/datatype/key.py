from .base import KafkaDataType


class KafkaKey(KafkaDataType):
    def _get_postfix(self) -> str:
        return "key"
