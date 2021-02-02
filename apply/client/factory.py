from meta.model import DBNode
from apply.client.aiomysql import AIOMySQL
from apply.client.client import Client
from common.dsn import DSN, Platform


class Factory(object):

    @staticmethod
    def new(node: DBNode) -> Client:
        dsn = DSN(node.dsn)
        if dsn.platform is Platform.MariaDB:
            return AIOMySQL.new(dsn, node.minimum_pool_size,
                                node.maximum_pool_size, node.wait_time)
        elif dsn.platform is Platform.MySQL:
            return AIOMySQL.new(dsn, node.minimum_pool_size,
                                node.maximum_pool_size, node.wait_time)
        else:
            raise Exception("unknown platform [{}].".format(dsn.platform.name))
