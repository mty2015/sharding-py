from shardingpy.parsing.lexer.lexerengine import LexerEngineFactory
from shardingpy.parsing.parser.sql.parser_factory import SQLParserFactory


class SQLParsingEngine:
    def __init__(self, db_type, sql, sharding_rule):
        self.db_type = db_type
        self.sql = sql
        self.sharding_rule = sharding_rule

    def parse(self):
        """
        return: SQLStatement
        """
        lexer_engine = LexerEngineFactory.new_instance(self.db_type, self.sql)
        lexer_engine.next_token()
        return SQLParserFactory.new_instance(self.db_type, lexer_engine.get_current_token().token_type,
                                             self.sharding_rule, lexer_engine).parse()
