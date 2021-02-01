from meta.constant import DBNodeType
from typing import Optional

from mq.factory import Factory as MQFactory
from common.config import Config, MQConfig
from common.change_event import ChangeEvent, ChangeEventLSN, EventType
from apply.client.factory import Factory as ClientFactory
from meta.manager import MetaManager


class Apply(object):
    def __init__(self):
        self.config = Config.get_instance()
        self.zone_id = self.config.current_zone_id
        self.meta_manager = MetaManager.new(self.config)
        self.current_file_log: str
        self.current_file_log_index: int
        self.current_log_pos: int
        self._prev_event_lsn: Optional[ChangeEventLSN] = None
        self.get_latest()
        self._running = True

    def get_latest(self):
        self.current_version = self.meta_manager.get_latest_version()
        self.create_mq()
        self.create_client(self.current_version)

    def create_mq(self):
        self.mq = MQFactory.new_consumer(MQConfig.get_instance())

    def create_client(self, version: int):
        db_conf = self.meta_manager.get_db(version)
        assert all((db_conf, db_conf.nodes))
        if self.config.node not in db_conf.nodes.keys():
            raise Exception("unknown node [{}].".format(self.config.node))
        node = db_conf.nodes[self.config.node]
        if node.type is DBNodeType.REPLICA:
            raise Exception("node [{}] is replica.".format(node.name))
        self.node = node
        self.client = ClientFactory.new(node)

    async def start(self):
        pass
