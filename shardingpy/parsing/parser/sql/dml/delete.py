from shardingpy.constant import SQLType, DatabaseType
from shardingpy.exception import UnsupportedOperationException
from shardingpy.parsing.lexer.token import DefaultKeyword
from shardingpy.parsing.parser.sql import SQLStatement


def new_delete_parser(db_type, sharding_rule, lexer_engine):
    if db_type == DatabaseType.MySQL:
        from shardingpy.parsing.parser.dialect.mysql import MySQLDeleteParser
        return MySQLDeleteParser(sharding_rule, lexer_engine)
    else:
        raise UnsupportedOperationException("Cannot support database {}".format(db_type))


class DeleteStatement(SQLStatement):
    def __init__(self):
        super().__init__(SQLType.DML)


class AbstractDeleteParser:
    def __init__(self, sharding_rule, lexer_engine, delete_clause_parser_facade):
        self.sharding_rule = sharding_rule
        self.lexer_engine = lexer_engine
        self.delete_clause_parser_facade = delete_clause_parser_facade

    def parse(self):
        self.lexer_engine.next_token()
        self.lexer_engine.skip_all(*self.get_skipped_keywords_between_delete_and_table())
        result = DeleteStatement()
        self.delete_clause_parser_facade.table_references_clause_parser.parse(result, True)
        self.lexer_engine.skip_until(DefaultKeyword.WHERE)
        self.delete_clause_parser_facade.where_clause_parser.parse(self.sharding_rule, result, [])
        return result

    def get_skipped_keywords_between_delete_and_table(self):
        return []
