from common.config import MQConfig
from kafka import KafkaConsumer, KafkaProducer

from mq.mq import MQConsumer, MQProducer


class KafkaP(MQProducer):

    @classmethod
    def new(cls, conf: MQConfig) -> 'KafkaP':
        c = cls(conf)
        return c

    def __init__(self, conf: MQConfig):
        self.timeout = conf.timeout
        self.topic = conf.topic
        self.kafka = KafkaProducer(
                bootstrap_servers=conf.bootstrap_servers,
                client_id=conf.client_id,
                acks=conf.acks)

    def send(self, table: bytes, value: bytes):
        f = self.kafka.send(self.topic, key=table, value=value)
        return f.get(timeout=self.timeout)

    def close(self):
        self.kafka.close()


class KafkaC(MQConsumer):

    @classmethod
    def new(cls, conf: MQConfig) -> 'KafkaC':
        c = cls(conf)
        return c

    def __init__(self, conf: MQConfig):
        self.topic = conf.topic
        self.kafka = KafkaConsumer(
                bootstrap_servers=conf.bootstrap_servers,
                client_id=conf.client_id,
                group_id=conf.group_id,
                auto_commit_interval_ms=conf.auto_commit_interval_ms)

    def close(self):
        self.kafka.close()
