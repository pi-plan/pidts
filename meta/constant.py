import enum


@enum.unique
class DBNodeType(enum.IntEnum):
    SOURCE = 1
    REPLICA = 2

    @classmethod
    def name2value(cls, name: str) -> 'DBNodeType':
        for member in list(cls):
            if member.name == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))


@enum.unique
class DBTableType(enum.IntEnum):
    RAW = 1
    SHARDING = 2
    DOUBLE_SHARDING = 3
    SYNC_TABLE = 4

    @classmethod
    def name2value(cls, name: str) -> 'DBTableType':
        for member in list(cls):
            if member.name == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))


@enum.unique
class RuleStatus(enum.IntEnum):
    BLOCK = 1
    RESHARDING = 2
    ACTIVE = 3

    @classmethod
    def name2value(cls, name: str) -> 'RuleStatus':
        for member in list(cls):
            if member.name == name.upper():
                return member

        raise Exception("type [{}] is not supported.".format(name))
