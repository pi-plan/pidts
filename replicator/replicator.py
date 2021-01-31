from replicator.listener.factory import Factory
from replicator.listener.mysql import Listener
from common.config import Config

from meta.manager import MetaManager


class Replicator(object):
    def __init__(self):
        self.config = Config.get_instance()
        self.zone_id = self.config.current_zone_id
        self.meta_manager = MetaManager.new(self.config)
        self.listener: Listener
        self.get_latest()

    def get_latest(self):
        self.current_version = self.meta_manager.get_latest_version()
        self.create_listeners(self.current_version)

    def create_listeners(self, version: int):
        db_conf = self.meta_manager.get_db(version)
        assert all((db_conf, db_conf.nodes))
        if self.config.node not in db_conf.nodes.keys():
            raise Exception("unknown node [{}].".format(self.config.node))
        node = db_conf.nodes[self.config.node]
        self.node = Factory.new(node, self.config.server_id)

    def start(self):
        # TODO 同步的进度上报到 pimms
        stream = self.node.get_stream(None, 0)
        for i in stream:
            i.dump()
