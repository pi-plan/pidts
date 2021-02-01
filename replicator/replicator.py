from typing import Optional

from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import WriteRowsEvent, DeleteRowsEvent,\
        UpdateRowsEvent

from replicator.listener.factory import Factory
from mq.factory import Factory as MQFactory
from common.config import Config, MQConfig
from common.change_event import ChangeEvent, ChangeEventLSN, EventType

from meta.manager import MetaManager


class Replicator(object):
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
        self.create_listeners(self.current_version)
        self.create_mq()

    def create_mq(self):
        self.mq = MQFactory.new_producer(MQConfig.get_instance())

    def create_listeners(self, version: int):
        db_conf = self.meta_manager.get_db(version)
        assert all((db_conf, db_conf.nodes))
        if self.config.node not in db_conf.nodes.keys():
            raise Exception("unknown node [{}].".format(self.config.node))
        node = db_conf.nodes[self.config.node]
        self.node = node
        self.node_stream = Factory.new(node, self.config.server_id)

    def start(self):
        log_file, log_pos = self.meta_manager.get_client()\
                .get_replicator_process(self.zone_id, self.node.name)
        if log_file is not None:
            self.current_file_log = log_file
            self.current_log_pos = log_pos
        stream = self.node_stream.get_stream(log_file, log_pos)
        i = 0
        for event in stream:
            i += 1
            if not self._running:
                break
            if isinstance(event, RotateEvent):
                self._rotate_event(event)
            elif isinstance(event, WriteRowsEvent):
                self._insert_event(event)
            elif isinstance(event, DeleteRowsEvent):
                self._delete_event(event)
            elif isinstance(event, UpdateRowsEvent):
                self._update_event(event)
            else:
                pass
            if i == 20:
                self._report_process()
                i = 0

    def _rotate_event(self, event: RotateEvent):
        self.current_log_pos = event.position
        file_log = event.next_binlog
        self.current_file_log = file_log
        log_index = ""
        for i in file_log[::-1]:
            if i.isdigit():
                log_index = i + log_index
            else:
                break
        self.current_file_log_index = int(log_index)
        self._report_process()

    def _report_process(self):
        try:
            self.meta_manager.get_client()\
                .report_replicator_process(self.zone_id, self.node.name,
                                           self.current_file_log,
                                           self.current_log_pos)
        except Exception:
            # 同步进度的时候遇到异常忽略下次会同步的。
            return

    def _update_event(self, event: UpdateRowsEvent):
        values = []
        for row in event.rows:
            if "pidal_c" not in row["after_values"].keys():
                continue
            if self._is_current_zone(row["after_values"]["pidal_c"]):
                values.append(row)
        if not values:
            return
        c_event = ChangeEvent.new(self._prev_event_lsn,
                                  self.zone_id, self.node.name, event,
                                  EventType.UPDATE,
                                  self.current_file_log_index,
                                  values)
        self.mq.send(c_event.table.encode(), c_event.encode())
        self._prev_event_lsn = c_event.lsn

    def _delete_event(self, event: DeleteRowsEvent):
        values = []
        for row in event.rows:
            if "pidal_c" not in row["values"].keys():
                continue
            if self._is_current_zone(row["values"]["pidal_c"]):
                values.append(row)
        if not values:
            return
        c_event = ChangeEvent.new(self._prev_event_lsn,
                                  self.zone_id, self.node.name, event,
                                  EventType.DELETE,
                                  self.current_file_log_index,
                                  values)
        self.mq.send(c_event.table.encode(), c_event.encode())
        self._prev_event_lsn = c_event.lsn

    def _insert_event(self, event: WriteRowsEvent):
        values = []
        for row in event.rows:
            if "pidal_c" not in row["values"].keys():
                continue
            if self._is_current_zone(row["values"]["pidal_c"]):
                values.append(row)
        if not values:
            return
        c_event = ChangeEvent.new(self._prev_event_lsn,
                                  self.zone_id, self.node.name, event,
                                  EventType.INSERT,
                                  self.current_file_log_index,
                                  values)
        self.mq.send(c_event.table.encode(), c_event.encode())
        self._prev_event_lsn = c_event.lsn

    def _is_current_zone(self, pidal_c: int) -> bool:
        source_zone_id = pidal_c >> 53
        return self.zone_id == source_zone_id
