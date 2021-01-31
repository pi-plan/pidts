from meta.model import DBNode
from replicator.listener.dsn import DSN, Platform
from replicator.listener.listener import Listener
from replicator.listener.mysql import MySQL


class Factory(object):

    @staticmethod
    def new(node: DBNode, server_id: int) -> Listener:
        dsn = DSN(node.dsn)
        if dsn.platform is Platform.MariaDB:
            return MySQL.new(node, dsn, server_id)
        elif dsn.platform is Platform.MySQL:
            return MySQL.new(node, dsn, server_id)
        else:
            raise Exception("Unknown platform [{}]".format(node.dsn))
