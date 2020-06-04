import re

from .base import BaseTopic


class RegexTopic(BaseTopic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pattern = re.compile(self.topic_name)

    def match(self, topic_name: str) -> bool:
        return self.pattern.match(topic_name)
