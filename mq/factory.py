from mq.mq import MQConsumer, MQProducer
from common.config import MQConfig
from mq.kafka import KafkaC, KafkaP


class Factory(object):
    @staticmethod
    def new_producer(conf: MQConfig) -> MQProducer:
        if conf.type == "kafka":
            return KafkaP.new(conf)
        else:
            raise Exception("unknown mq type")

    @staticmethod
    def new_consumer(conf: MQConfig) -> MQConsumer:
        if conf.type == "kafka":
            return KafkaC.new(conf)
        else:
            raise Exception("unknown mq type")
