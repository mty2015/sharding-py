import enum


class DatabaseType(enum.Enum):
    H2 = "h2"
    MySQL = "MySQL"


class AggregationType(enum.IntEnum):
    MAX = 1
    MIN = 2
    SUM = 3
    COUNT = 4
    AVG = 5


class SQLType(enum.IntEnum):
    DQL = 1
    DML = 2
    DDL = 3


class ShardingOperator(enum.Enum):
    EQUAL = "="
    BETWEEN = "BETWEEN"
    IN = "IN"
