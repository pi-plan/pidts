import abc
from typing import Any, Dict, List

from aiomysql.connection import Connection
from common.dsn import DSN


class Client(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractclassmethod
    def new(cls, dsn: DSN, minsize: int = 1, maxsize: int = 10,
            pool_recycle: int = -1) -> 'Client':
        pass

    @abc.abstractmethod
    async def execute(self, sql: str) -> int:
        pass

    @abc.abstractmethod
    async def query(self, sql: str) -> List[Dict[str, Any]]:
        pass

    @abc.abstractmethod
    async def connect(self):
        pass

    @abc.abstractmethod
    async def acquire(self) -> Connection:
        # TODO 抽象 Client
        pass

    @abc.abstractmethod
    def release(self, conn: Connection):
        pass

    @abc.abstractmethod
    def close(self):
        pass
