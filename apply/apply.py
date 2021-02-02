from common.algorithms import Algorithm
from typing import Any, Dict, List, Optional, Tuple

from common.logging import logger
from mq.factory import Factory as MQFactory
from common.config import Config, MQConfig
from common.change_event import ChangeEvent, ChangeEventLSN, EventType
from apply.client.factory import Factory as ClientFactory
from meta.manager import MetaManager
from meta.constant import DBNodeType


class Apply(object):
    def __init__(self):
        self.config = Config.get_instance()
        self.zone_id = self.config.current_zone_id
        self.meta_manager = MetaManager.new(self.config)
        self.current_file_log: str
        self.current_file_log_index: int
        self.current_log_pos: int
        self._last_lsn: Optional[ChangeEventLSN] = None
        self._running = True
        self.tables: Dict[str, List[str]] = {}
        self.table_zs_algorithms: Dict[str, Dict[str, Any]] = {}
        self.zsid: Dict[int, int] = {}

        self.get_latest()

    def get_latest(self):
        self.current_version = self.meta_manager.get_latest_version()
        self.create_mq()
        self.create_client(self.current_version)
        self.parse_zsid()

    async def get_tables(self):
        sql = "SHOW KEYS FROM {} WHERE Non_unique = 0 and Key_name = '{}'"
        db_conf = self.meta_manager.get_db(self.current_version)
        for i in db_conf.tables.values():
            raw_tables = []
            if not i.strategies:
                continue
            for s in i.strategies:
                if not s.backends:
                    continue
                for n in s.backends:
                    if n.node == self.node.name:
                        if n.number is None:
                            raw_tables.append(n.prefix)
                        else:
                            raw_tables.append(n.prefix + str(n.number))
                    else:
                        break

            if raw_tables:
                r = await self.client.query(sql.format(raw_tables[0],
                                                       i.lock_key))
                if not r:
                    raise Exception("unknown table {} key {}".format(
                        raw_tables[0], i.lock_key))
                _raw_lock_keys = {}
                for k in r:
                    _raw_lock_keys[k["Seq_in_index"]] = k["Column_name"]
                lock_key = [_raw_lock_keys[k] for k in sorted(
                    _raw_lock_keys.keys())]
                zs_algorithm = {}
                zs_algorithm["zskeys"] = i.zskeys
                zs_algorithm["zs_algorithm"] = Algorithm.new(i.zs_algorithm)
                zs_algorithm["zs_algorithm_args"] = i.zs_algorithm_args
                for t in raw_tables:
                    self.tables[t] = lock_key
                    self.table_zs_algorithms[t] = zs_algorithm

    def create_mq(self):
        self.mq = MQFactory.new_consumer(MQConfig.get_instance())

    def create_client(self, version: int):
        db_conf = self.meta_manager.get_db(version)
        assert all((db_conf, db_conf.nodes, db_conf.tables))
        if self.config.node not in db_conf.nodes.keys():
            raise Exception("unknown node [{}].".format(self.config.node))
        node = db_conf.nodes[self.config.node]
        if node.type is DBNodeType.REPLICA:
            raise Exception("node [{}] is replica.".format(node.name))
        self.node = node
        self.client = ClientFactory.new(node)

    def parse_zsid(self):
        zones = self.meta_manager.get_zones(self.current_version)
        for i in zones:
            if not i.shardings:
                continue
            for s in i.shardings:
                self.zsid[s.zsid] = i.zone_id

    async def start(self):
        await self.client.connect()
        await self.get_tables()
        for i in self.mq.get_stream():
            if not i.value:
                continue
            event = ChangeEvent.decode(i.value.decode())
            if not self._check_lsn(event):
                raise Exception("event has miss [{}]".format(
                    event.lsn.encode()))
            if event.table not in self.tables.keys():
                raise Exception("unknown table [{}] in event lsn{}:".format(
                    event.table, event.lsn.encode()))
            if self._last_lsn == event.lsn:  # 重复消费
                continue
            if event.event_type is EventType.DELETE:
                await self._delete(event)
            elif event.event_type is EventType.INSERT:
                await self._insert(event)
            elif event.event_type is EventType.UPDATE:
                await self._update(event)
            else:
                continue
            self._last_lsn = event.lsn

    async def _update(self, event: ChangeEvent):
        get_current_sql = "SELECT * FROM {} WHERE {} FOR UPDATE"
        lock_sql = "UPDATE {} set `pidal_c` = `pidal_c` | 1 WHERE {}"
        update_sql = "UPDATE {} set {} WHERE {}"
        client = await self.client.acquire()
        try:
            for i in event.values:
                if "before_values" not in i.keys() or "after_values" not in i.keys():
                    raise Exception("update event value {} error lsn{}.".format(i, event.lsn.encode()))
                if not self._check_lock(event.table, i["before_values"]) or not self._check_lock(event.table, i["after_values"]):
                    raise Exception("need lock_key {} error lsn{}.".format(self.tables[event.table], event.lsn.encode()))

                where = self._parser_where(self.tables[event.table], i["before_values"])
                await client.begin()
                async with client.cursor() as cur:
                    await cur.execute(get_current_sql.format(event.table, where))
                    current = await cur.fetchone()
                    if current == i["after_values"]:  # 数据已经修改过了。
                        await client.rollback()
                        continue
                    belong_zone_id = self._get_belong_zone_id(event.table, i["before_values"])
                    if belong_zone_id == self.zone_id:
                        # 自己的修改
                        continue
                    source_zone_id, _, data_version, _ = self._parser_pidal_c(i["after_values"]["pidal_c"])
                    _, _, current_version, is_lock = self._parser_pidal_c(current["pidal_c"])
                    if belong_zone_id != source_zone_id:
                        logger.error("error data modify in zone_id {} table {} data {}".format(event.source_zone_id, event.table, i["before_values"]))
                        if belong_zone_id == self.zone_id and not is_lock:
                            await cur.execute(lock_sql.format(event.table, where))
                            await client.commit()
                        continue
                    if current == i["before_values"]:
                        await cur.execute(update_sql.format(event.table, self._parser_set(i["after_values"]), where))
                        await client.commit()
                        continue
                    if data_version < current_version:
                        # 老数据
                        await client.rollback()
                    elif data_version == current_version:
                        # 数据校验不一致
                        if belong_zone_id == self.zone_id and not is_lock:
                            await cur.execute(lock_sql.format(event.table, where))
                            await client.commit()
                        else:
                            await client.rollback()
                    else:
                        # 走到这里可能是数据落后版本太多,直接覆盖
                        await cur.execute(update_sql.format(event.table, self._parser_set(i["after_values"]), where))
                        await client.commit()
        finally:
            await client.rollback()
            self.client.release(client)

    @staticmethod
    def _parser_set(values: Dict[str, Any]) -> str:
        r = []
        for k, v in values.items():
            r.append(" `{}` = '{}' ".format(k, v))
        return ",".join(r)

    def _get_belong_zone_id(self, table: str, values: Dict[str, Any]) -> int:
        a = self.table_zs_algorithms[table]
        args = []
        if a["zs_algorithm_args"]:
            args.append(a["zs_algorithm_args"].copy())
        for i in a["zskeys"]:
            args.append(values[i])
        zsid = a["zs_algorithm"](*args)
        return self.zsid[zsid]

    @staticmethod
    def _parser_pidal_c(pidal_c: int) -> Tuple[int, int, int, bool]:
        p = bin(pidal_c)
        version = p[-21:-1]
        meta_version = p[-53:-33]
        return (pidal_c >> 53, int(meta_version, 2), int(version, 2),
                not bool(pidal_c | 1))

    @staticmethod
    def _parser_where(lock_key: List[str], values: Dict[str, Any]) -> str:
        return "AND".join(
                [" `{}` = '{}' ".format(i, values[i]) for i in lock_key])

    def _check_lock(self, table: str, values: Dict[str, Any]) -> bool:
        keys = values.keys()
        for i in self.tables[table]:
            if i not in keys:
                return False
        return True

    async def _insert(self, event: ChangeEvent):
        get_current_sql = "SELECT * FROM {} WHERE {} FOR UPDATE"
        insert_sql = "INSERT INTO {} ({}) VALUES ({})"
        client = await self.client.acquire()
        try:
            for i in event.values:
                if "values" not in i.keys():
                    raise Exception("update event value {} error lsn{}.".format(i, event.lsn.encode()))
                if not self._check_lock(event.table, i["values"]):
                    raise Exception("need lock_key {} error lsn{}.".format(self.tables[event.table], event.lsn.encode()))

                where = self._parser_where(self.tables[event.table], i["values"])
                await client.begin()
                async with client.cursor() as cur:
                    await cur.execute(get_current_sql.format(event.table, where))
                    current = await cur.fetchone()
                    if current == i["values"]:  # 数据已经插入。
                        await client.rollback()
                        continue
                    if not current:
                        # 正常插入数据
                        cl = ", ".join(["`{}`".format(c) for c in i["values"].keys()])
                        va = ", ".join(["'{}'".format(c) for c in i["values"].values()])
                        await cur.execute(insert_sql.format(event.table, cl, va))
                        await client.commit()
                    # 走到这里说明有数据，切和 event 的不一致。
                    belong_zone_id = self._get_belong_zone_id(event.table, i["values"])
                    if belong_zone_id == self.zone_id:
                        # 自己的修改
                        await client.rollback()
                        continue
                    source_zone_id, _, data_version, _ = self._parser_pidal_c(i["values"]["pidal_c"])
                    if belong_zone_id != source_zone_id:
                        logger.error("error data insert  in zone_id {} table {} data {}".format(event.source_zone_id, event.table, i["before_values"]))
                        await client.rollback()
                        continue
                    # 走到这里说明来源的数据是对的，但是当前的数据不知道什么原因保留现场，不自改。
                    logger.error("error data insert in zone_id {} table {} data {}".format(self.zone_id, event.table, current))
                    await client.rollback()
        finally:
            await client.rollback()
            self.client.release(client)

    async def _delete(self, event: ChangeEvent):
        get_current_sql = "SELECT * FROM {} WHERE {} FOR UPDATE"
        delete_sql = "DELETE FROM {} WHERE {}"
        client = await self.client.acquire()
        try:
            for i in event.values:
                if "values" not in i.keys():
                    raise Exception("update event value {} error lsn{}.".format(i, event.lsn.encode()))
                if not self._check_lock(event.table, i["values"]):
                    raise Exception("need lock_key {} error lsn{}.".format(self.tables[event.table], event.lsn.encode()))

                where = self._parser_where(self.tables[event.table], i["values"])
                await client.begin()
                async with client.cursor() as cur:
                    await cur.execute(get_current_sql.format(event.table, where))
                    current = await cur.fetchone()
                    if not current:  # 数据已经删除
                        await client.rollback()
                        continue
                    await cur.execute(delete_sql.format(event.table, where))
                    await client.commit()
                    if current != i["values"]:  # 需要删除数据
                        # 走到这里说明有数据，切和 event 的不一致。
                        logger.error("error data insert in zone_id {} table {} data {}".format(self.zone_id, event.table, current))
        finally:
            await client.rollback()
            self.client.release(client)

    def _check_lsn(self, event: ChangeEvent) -> bool:
        if event.is_retry:
            return True
        if not self._last_lsn:
            return True
        if self._last_lsn == event.prev_lsn:
            return True
        if self._last_lsn == event.lsn:  # 消息重复
            return True
        return False
