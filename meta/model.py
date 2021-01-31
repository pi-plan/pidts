import re

from typing import Dict, List, Any, Optional

from meta.constant import DBNodeType, DBTableType, RuleStatus

TABLE_NAME_EXP_RE = re.compile(r"^([\w.]+)_{(\d+)\,\s*(\d+)\,*\s*(\d?)}$")
TABLE_NAME_NUM_RE = re.compile(r"^([\w.]+)_(\d+)$")


class ZoneConfig(object):
    def __init__(self, zone_id: int, zone_name: str,
                 shardings: List['ZoneSharding'],
                 db: Optional['DBConfig'] = None):
        self.zone_id: int = zone_id
        self.zone_name: str = zone_name
        self.shardings: List[ZoneSharding] = shardings
        self.db: Optional[DBConfig] = db

    @classmethod
    def new_from_dict(cls, conf: Dict[str, Any]):
        shardings: List[ZoneSharding] = []
        for i in conf["shardings"]:
            sharding = ZoneSharding.new_from_dict(i)
            shardings.append(sharding)

        db = None
        if "db" in conf.keys():
            db = DBConfig.new_from_dict(conf["db"])
        zc = cls(conf["zone_id"], conf["zone_name"], shardings, db)

        return zc


class ZoneSharding(object):
    def __init__(self, zsid: int, status: RuleStatus):
        self.zsid: int = int(zsid)
        self.status: RuleStatus = status

    @classmethod
    def new_from_dict(cls, conf: dict) -> 'ZoneSharding':
        if isinstance(conf["status"], int):
            status = RuleStatus(conf["status"])
        else:
            status = RuleStatus.name2value(conf["status"])
        zs = cls(conf["zsid"], status)
        return zs


class DBConfig(object):
    def __init__(self,
                 name: str,
                 source_replica_enable: bool,
                 algorithm: str,
                 algorithm_args: Optional[List[Any]] = None,
                 transaction_mod: str = "simple",
                 idle_in_transaction_session_timeout: int = 5000):
        self.name: str = name
        self.source_replica_enable: bool = source_replica_enable
        self.algorithm = algorithm
        self.algorithm_args = algorithm_args
        self.transaction_mod = transaction_mod
        self.nodes: Dict[str, DBNode] = dict()
        self.tables: Dict[str, DBTable] = dict()

        # 事务空闲超时时间，单位毫秒, 默认 5000ms
        self.idle_in_transaction_session_timeout: int \
            = idle_in_transaction_session_timeout

    @classmethod
    def new_from_dict(cls, conf: dict) -> 'DBConfig':
        transaction_mod = str(conf.get("transaction_mod", "simple"))
        idle_in_transaction_session_timeout = conf.get(
                "idle_in_transaction_session_timeout", 0)
        dbc = cls(conf["name"],
                  conf["source_replica"]["enable"],
                  conf["source_replica"]["algorithm"],
                  conf["source_replica"]["algorithm_args"],
                  transaction_mod, idle_in_transaction_session_timeout)

        for i in conf["nodes"]:
            node = DBNode.new_from_dict(i)
            if node.name in dbc.nodes.keys():
                raise Exception("[{}] node has defined.".format(node.name))
            dbc.nodes[node.name] = node

        for i in conf["tables"]:
            table = DBTable.new_from_dict(i)
            if table.name in dbc.tables.keys():
                raise Exception("[{}] table has defined.".format(table.name))
            dbc.tables[table.name] = table
        return dbc


class DBTable(object):
    def __init__(self, type: DBTableType, name: str,
                 status: RuleStatus, zskeys: List[str], zs_algorithm: str,
                 zs_algorithm_args: Optional[List[Any]], lock_key: str,
                 strategies: List['DBTableStrategy']):
        self.type: DBTableType = type
        self.name: str = name
        self.status: RuleStatus = status
        self.zskeys = zskeys
        self.zs_algorithm = zs_algorithm
        self.zs_algorithm_args = zs_algorithm_args
        # 数据写入的时候负责锁的索引，
        # SHOW KEYS FROM table where Non_unique = 0 and Key_name = "PRIMARY";
        self.lock_key: str = lock_key
        self.strategies: List[DBTableStrategy] = strategies

    @classmethod
    def new_from_dict(cls, conf: dict) -> 'DBTable':
        strategies: List['DBTableStrategy'] = []
        for i in conf["strategies"]:
            s = DBTableStrategy.new_from_dict(i)
            strategies.append(s)
        if isinstance(conf["type"], int):
            type = DBTableType(conf["type"])
        else:
            type = DBTableType.name2value(conf["type"])
        if isinstance(conf["status"], int):
            status = RuleStatus(conf["status"])
        else:
            status = RuleStatus.name2value(conf["status"])
        dbt = cls(type, conf["name"], status, conf["zskeys"],
                  conf["zs_algorithm"], conf["zs_algorithm_args"],
                  conf["lock_key"], strategies)

        return dbt


class DBTableStrategy(object):
    def __init__(self,
                 backends: List['DBTableStrategyBackend'],
                 sharding_columns: Optional[List[str]],
                 algorithm: Optional[str],
                 algorithm_args: Optional[List[Any]] = None):
        self.backends: List[DBTableStrategyBackend] = backends
        self.sharding_columns: Optional[List[str]] = sharding_columns
        self.algorithm: Optional[str] = algorithm
        self.algorithm_args = algorithm_args

    @classmethod
    def new_from_dict(cls, conf: dict) -> 'DBTableStrategy':
        backends: List[DBTableStrategyBackend] = []
        for i in conf["backends"]:
            bs = DBTableStrategyBackend.number_expression(i)
            if bs:
                backends.extend(bs)
                continue
            t = DBTableStrategyBackend.parser_tablename(i)
            if t:
                backends.append(t)
                continue
            t = DBTableStrategyBackend.parser_raw_table(i)
            if t:
                backends.append(t)

        sharding_columns = None
        if "sharding_columns" in conf.keys():
            sharding_columns = conf["sharding_columns"]
        algorithm = conf["algorithm"] if "algorithm" in conf.keys() else None
        algorithm_args = conf["algorithm_args"] if "algorithm_args" in \
            conf.keys() else None
        return cls(backends, sharding_columns, algorithm, algorithm_args)


class DBTableStrategyBackend(object):
    def __init__(self, node: str, prefix: str, number: Optional[int]):
        self.node: str = node
        self.prefix: str = prefix
        self.number = number

    @classmethod
    def number_expression(cls, expression: str) -> \
            Optional[List['DBTableStrategyBackend']]:
        value = TABLE_NAME_EXP_RE.findall(expression)
        if not value:
            return None
        if len(value) > 1:
            raise Exception("expression: [{}] has two expression".format(
                expression))
        value = [i for i in value[0] if i]
        if len(value) < 3:
            raise Exception("expression: [{}] need stop.".format(
                expression))
        if len(value) > 4:
            raise Exception("expression: [{}] only need start, stop, step.\
".format(expression))
        base = [i for i in str(value[0]).split(".", 1) if i]
        if len(base) != 2:
            raise Exception("expression: [{}] need [node.table].".format(
                expression))
        node = base[0]
        prefix = base[1] + "_"
        start = int(value[1])
        stop = int(value[2])
        step = 1
        if len(value) > 3:
            step = int(value[3])

        result = []
        for i in range(start, stop, step):
            result.append(cls(node, prefix, i))

        return result

    @classmethod
    def parser_tablename(cls, expression: str) -> \
            Optional['DBTableStrategyBackend']:
        value = TABLE_NAME_NUM_RE.findall(expression)
        if not value:
            return None
        if len(value) > 1:
            raise Exception("expression: [{}] has two numbers.".format(
                expression))
        value = [i for i in value[0] if i]
        if len(value) != 2:
            raise Exception("expression: [{}] need one number.".format(
                expression))

        base = [i for i in str(value[0]).split(".", 1) if i]
        if len(base) != 2:
            raise Exception("expression: [{}] need [node.table].".format(
                expression))
        node = base[0]
        prefix = base[1] + "_"
        return cls(node, prefix, int(value[1]))

    @classmethod
    def parser_raw_table(cls, table_name: str) -> \
            Optional['DBTableStrategyBackend']:
        base = [i for i in table_name.split(".", 1) if i]
        if len(base) != 2:
            raise Exception("expression: [{}] need [node.table].".format(
                table_name))
        node = base[0]
        table = base[1]
        return cls(node, table, None)


class DBNode(object):
    def __init__(self, type: DBNodeType, name: str, dsn: str,
                 minimum_pool_size: int = 1, maximum_pool_size: int = 100,
                 wait_time: int = 3600, acquire_timeout: int = 5,
                 follow: str = None):
        self.type: DBNodeType = type
        self.name: str = name
        self.dsn: str = dsn
        self.minimum_pool_size: int = max(minimum_pool_size, 1)
        self.maximum_pool_size: int = maximum_pool_size
        self.wait_time: int = wait_time  # 最长空闲时间
        self.acquire_timeout: int = acquire_timeout
        self.follow: Optional[str] = follow

    @classmethod
    def new_from_dict(cls, conf: dict) -> 'DBNode':
        if isinstance(conf["type"], int):
            status = DBNodeType(conf["type"])
        else:
            status = DBNodeType.name2value(conf["type"])
        del(conf["type"])
        dbn = cls(status, **conf)
        return dbn
