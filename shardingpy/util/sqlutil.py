import re

from shardingpy.constant import DatabaseType
from shardingpy.parsing.lexer.dialect.mysql import MySQLKeyword
from shardingpy.parsing.lexer.token import DefaultKeyword


def get_exactly_value(value):
    return re.sub(r'[\[\]`\'"]', '', value) if value else None


def get_original_value(value, database_type):
    if database_type != DatabaseType.MySQL:
        return value

    try:
        DefaultKeyword[value.upper()]
        return "`%s`" % value
    except KeyError:
        return _get_original_value_for_mysql_keyword(value)


def _get_original_value_for_mysql_keyword(value):
    try:
        MySQLKeyword[value.upper()]
        return "`%s`" % value
    except KeyError:
        return value
