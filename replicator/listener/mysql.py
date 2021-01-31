from typing import Optional
from replicator.listener.listener import Listener
from pymysqlreplication import BinLogStreamReader

from common.dsn import DSN
from meta.model import DBNode


class MySQL(Listener):

    @classmethod
    def new(cls, node: DBNode, dsn: DSN, server_id: int) -> 'Listener':
        return cls(node, dsn, server_id)

    def __init__(self, node: DBNode, dsn: DSN, server_id: int):
        self.node = node
        self.dsn = dsn
        self.server_id = server_id

    def get_stream(self, log_file: Optional[str],
                   log_pos: int) -> BinLogStreamReader:
        blocking = False
        if log_file is None:
            self.stream = BinLogStreamReader(
                    connection_settings=self.dsn.get_args(),
                    server_id=self.server_id, blocking=blocking)
        else:
            self.stream = BinLogStreamReader(
                    connection_settings=self.dsn.get_args(),
                    server_id=self.server_id, blocking=blocking,
                    log_file=log_file, log_pos=log_pos)
        return self.stream

    def close(self):
        self.stream.close()
