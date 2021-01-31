import abc
from typing import Optional

from pymysqlreplication.binlogstream import BinLogStreamReader

from common.dsn import DSN
from meta.model import DBNode


class Listener(metaclass=abc.ABCMeta):

    @classmethod
    @abc.abstractclassmethod
    def new(cls, node: DBNode, dsn: DSN, server_id: int) -> 'Listener':
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def get_stream(self, log_file: Optional[str],
                   log_pos: int) -> BinLogStreamReader:
        pass
