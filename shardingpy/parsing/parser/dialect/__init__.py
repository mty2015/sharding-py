from shardingpy.constant import DatabaseType
from shardingpy.parsing.parser.dialect.mysql import MySQLAliasExpressionParser
from shardingpy.parsing.parser.expressionparser import BasicExpressionParser


def create_alias_expression_parser(lexer_engine):
    if lexer_engine.get_database_type == DatabaseType.MySQL:
        return MySQLAliasExpressionParser(lexer_engine)


def create_basic_expression_parser(lexer_engine):
    return BasicExpressionParser(lexer_engine)
