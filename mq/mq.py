import abc
from common.config import MQConfig


class MQProducer(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractclassmethod
    def new(cls, conf: MQConfig) -> 'MQProducer':
        pass

    @abc.abstractmethod
    def send(self, table: bytes, value: bytes):
        pass

    @abc.abstractmethod
    def close(self):
        pass


class MQConsumer(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractclassmethod
    def new(cls, conf: MQConfig) -> 'MQConsumer':
        pass

    @abc.abstractmethod
    def close(self):
        pass
