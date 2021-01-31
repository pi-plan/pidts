from typing import Any, Dict, List, Callable, Optional

from common.config import Config

from meta.model import DBConfig, ZoneConfig
from common.circular_version_number import CircularVersionNumber
from meta.client import Client

MAX_META_VERSION = 2 ** 20 - 1
META_VERSION_BUFFERSIZE = 1440


class MetaManager(object):

    _instance: Optional['MetaManager'] = None

    @classmethod
    def new(cls, config: Config) -> 'MetaManager':
        if cls._instance:
            return cls._instance
        c = cls(config)
        cls._instance = c
        c.init_latest()
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'MetaManager':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance

    def __init__(self, config: Config):
        self.versions: Dict[int, Dict[int, ZoneConfig]] = {}
        self.latest_version: int = 0
        self.config = config
        self.zone_id = config.current_zone_id

        # 元数据更新的观察者
        self.observers: List[Callable[[int]]] = []

        self._pimms_client: Client
        self._meta_version_tool = CircularVersionNumber(
                MAX_META_VERSION, META_VERSION_BUFFERSIZE)
        self.init_pimms_client()

    def get_latest_version(self) -> int:
        return self.latest_version

    def add_observer(self, handler: Callable[[int], None]):
        if handler in self.observers:
            return
        self.observers.append(handler)

    def init_pimms_client(self):
        self._pimms_client: Client = Client.new(self.config.get_meta_config())
        self._pimms_client.add_observer(self._on_version_update)

    def _on_version_update(self, new_version: int):
        for i in self.observers:
            i(new_version)

    def init_latest(self):
        latest_version = self._pimms_client.get_latest_version()
        if latest_version < self.latest_version:
            return
        if latest_version in self.versions.keys():
            return
        self.latest_version = latest_version
        self.load_version_meta(latest_version)

    def load_version_meta(self, version: int):
        if version in self.versions.keys():
            return
        zones_dict = self._pimms_client.get_zones(version)
        zone_list = self.parser_zone_config(zones_dict)
        zones = {}
        for zone in zone_list:
            if zone.zone_id in zones.keys():
                raise Exception("zone id [{}] has defined.".format(
                                zone.zone_id))
            if zone.zone_id == self.zone_id and not zone.db:
                db_conf = self._pimms_client.get_db(version, self.zone_id)
                zone.db = self.parser_db_config(db_conf)
            zones[zone.zone_id] = zone
        self.versions[version] = zones

    def version_isloaded(self, version: int) -> bool:
        return version in self.versions.keys()

    def get_zones(self, version: int = 0) -> List[ZoneConfig]:
        if version < 1:
            version = self.latest_version
        return list(self.versions[version].values())

    def get_db(self, version: int = 0, zone_id: int = 0) -> Optional[DBConfig]:
        if version < 1:
            version = self.latest_version
        if not zone_id:
            zone_id = self.zone_id
        zone = self.versions[version].get(zone_id)
        return zone.db

    def get_client(self) -> Client:
        return self._pimms_client

    def clean_ontime():  # type: ignore
        """ 定时清理过期的 version 数据 """
        # TODO
        pass

    @staticmethod
    def parser_zone_config(conf: List[dict]) -> List[ZoneConfig]:
        zones: List[ZoneConfig] = []
        for i in conf:
            zone = ZoneConfig.new_from_dict(i)
            zones.append(zone)
        return zones

    @staticmethod
    def parser_db_config(conf: Dict[str, Any]) -> DBConfig:
        return DBConfig.new_from_dict(conf)

    def version_gt(self, v1: int, v2: int) -> bool:
        return self._meta_version_tool.gt(v1, v2)
