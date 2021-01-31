import os

from typing import List, Any, Optional, Tuple, MutableMapping

import toml


class Config(object):
    """
    所有配置的factory，单例可以被修改
    """

    _instance: Optional['Config'] = None

    def __init__(self, current_zone_id: int, node: str, server_id: int):
        self.current_zone_id: int = int(current_zone_id)
        self.node = node
        self.server_id = server_id

    @classmethod
    def new(cls, current_zone_id: int, node: str, server_id: int):
        if cls._instance:
            del(cls._instance)
        c = cls(current_zone_id, node, server_id)
        cls._instance = c
        return c._instance

    @classmethod
    def get_instance(cls) -> 'Config':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance

    @staticmethod
    def get_meta_config() -> 'MetaService':
        return MetaService.get_instance()


class MetaService(object):
    """
    单例，不能被修改
    """

    _instance: Optional['MetaService'] = None

    def __init__(self,
                 servers: List[Tuple[str, int]],
                 wait_timeout: int):
        self.servers: List[Tuple[str, int]] = servers
        self.wait_timeout: int = wait_timeout

    @classmethod
    def new(cls,
            servers: List[Tuple[str, int]],
            wait_timeout: int) -> 'MetaService':
        if cls._instance:
            return cls._instance
        c = cls(servers, wait_timeout)
        cls._instance = c
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'MetaService':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance


class LoggingHandlerConfig(object):
    def __init__(self, class_name: str, args: List[List[Any]]):
        self.class_name: str = class_name
        self.args: List[List[Any]] = args


class LoggingConfig(object):
    """
    单例，不能被修改
    """

    _instance: Optional['LoggingConfig'] = None

    def __init__(self, level: str, format: str, datefmt: str,
                 handler: LoggingHandlerConfig):
        self.level = level
        self.format = format
        self.datefmt = datefmt
        self.handler = handler

    @classmethod
    def new(cls, level: str, format: str, datefmt: str,
            handler: LoggingHandlerConfig) -> 'LoggingConfig':
        if cls._instance:
            return cls._instance
        c = cls(level, format, datefmt, handler)
        cls._instance = c
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'LoggingConfig':
        if not cls._instance:
            raise Exception("Not yet initialized")
        return cls._instance


def parser_config(zone_id: int, file: str):
    with open(file, "r") as f:
        config = toml.load(f)
        for i in ["base"]:
            if i not in config:
                raise Exception("config file is error.")

        for i in ["logging", "meta_service"]:
            if i not in config["base"]:
                raise Exception("config file is error.")
        for i in ["level", "format", "datefmt", "handler"]:
            if i not in config["base"]["logging"]:
                raise Exception("config file is error.")

        for i in ["class", "args"]:
            if i not in config["base"]["logging"]["handler"]:
                raise Exception("config file is error.")

        logging_handler = LoggingHandlerConfig(
                config["base"]["logging"]["handler"]["class"],
                config["base"]["logging"]["handler"]["args"])
        LoggingConfig.new(
                config["base"]["logging"]["level"],
                config["base"]["logging"]["format"],
                config["base"]["logging"]["datefmt"],
                logging_handler)

        Config.new(_get_zone_id(zone_id, config["base"]),
                   config["base"]["node"], config["base"]["server_id"])
        for i in ["servers", "wait_timeout"]:
            if i not in config["base"]["meta_service"]:
                raise Exception("config file is error.")
        servers = []
        for i in config["base"]["meta_service"]["servers"]:
            servers.append((i["host"], i["port"]))

        MetaService.new(servers,
                        config["base"]["meta_service"]["wait_timeout"])


def _get_zone_id(zone_id: int, conf: MutableMapping[str, Any]) -> int:
    """
    获取当前的 zone id 优先级为 启动参数指定 > 配置文件指定 > 环境变量
    """
    if zone_id:
        return zone_id
    if "zone_id" in conf.keys():
        return int(conf["zone_id"])
    env_zone_id = os.environ.get("PIDAL_ZONE_ID")
    if env_zone_id:
        return int(env_zone_id)
    return 0
