import enum
import json

from typing import Any, Dict, List, Optional

from pymysqlreplication.row_event import RowsEvent


@enum.unique
class EventType(enum.IntEnum):
    INSERT = 1
    UPDATE = 2
    DELETE = 3


class ChangeEventLSN(object):
    def __init__(self, source_zone_change_no: int, server_id: int,
                 log_index: int,
                 log_position: int,
                 xid: int = 0):
        self.source_zone_change_no: int = source_zone_change_no
        self.server_id: int = server_id
        self.log_index: int = log_index
        self.log_position: int = log_position
        self.xid: int = xid

    def encode(self) -> Dict[str, Any]:
        return vars(self)

    @classmethod
    def decode(cls, data: Dict[str, Any]):
        return cls(data["source_zone_id"], data["server_id"],
                   data["log_index"],
                   data["log_position"], data["xid"])


class ChangeEvent(object):

    @classmethod
    def new(cls, prev_lsn: Optional[ChangeEventLSN],
            source_zone_id: int, node: str, event: RowsEvent,
            event_type: EventType, log_index: int,
            values: List[Dict[str, Dict[str, Any]]]) -> 'ChangeEvent':
        lsn = ChangeEventLSN(0, event.packet.server_id, log_index,
                             event.packet.log_pos)
        e = cls(prev_lsn, lsn, event.timestamp, event_type, source_zone_id,
                node, event.schema, event.table, values,
                prev_lsn is None)
        return e

    def __init__(self,
                 prev_lsn: Optional[ChangeEventLSN],
                 lsn: ChangeEventLSN,
                 timestamp: int,
                 event_type: EventType,
                 source_zone_id: int,
                 node: str,
                 db: str,
                 table: str,
                 values: List[Dict[str, Dict[str, Any]]],
                 is_retry: bool = False):
        self.lsn = lsn
        self.prev_lsn = prev_lsn
        self.timestamp = timestamp
        self.event_type = event_type
        self.source_zone_id = source_zone_id
        self.node = node
        self.db = db
        self.table = table
        self.values = values
        self.is_retry = is_retry

    def encode(self) -> bytes:
        data = {}
        data["lsn"] = self.lsn.encode()
        if self.prev_lsn:
            data["prev_lsn"] = self.prev_lsn.encode()
        else:
            data["prev_lsn"] = None
        data["timestamp"] = self.timestamp
        data["event_type"] = self.event_type.value
        data["source_zone_id"] = self.source_zone_id
        data["node"] = self.node
        data["db"] = self.db
        data["table"] = self.table
        data["values"] = self.values
        data["is_retry"] = self.is_retry
        j = json.dumps(data)
        return j.encode()

    @classmethod
    def decode(cls, data: str):
        d = json.loads(data)
        prev_lsn = None
        if d["prev_lsn"]:
            prev_lsn = ChangeEventLSN.decode(d["prev_lsn"])
        lsn = ChangeEventLSN.decode(d["lsn"])
        return cls(prev_lsn, lsn, d["timestamp"], d["event_type"],
                   d["source_zone_id"], d["node"], d["db"], d["table"],
                   d["values"], d["is_retry"])
