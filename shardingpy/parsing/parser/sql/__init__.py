from shardingpy.exception import SQLParsingUnsupportedException
from shardingpy.parsing.parser.context.condition import Conditions
from shardingpy.parsing.parser.context.table import Tables
from shardingpy.parsing.token import DefaultKeyword
from .dql.select import new_select_parser


class SQLStatement:
    def __init__(self, sql_type):
        self.sql_type = sql_type
        self.tables = Tables()
        self.conditions = Conditions()
        self.sql_tokens = list()
        self.parameters_index = 0

    def increase_parameters_index(self):
        self.parameters_index += 1


class SQLParserFactory:
    def new_instance(cls, db_type, token_type, sharding_rule, lexer_engine):
        if token_type == DefaultKeyword.SELECT:
            return new_select_parser(db_type, sharding_rule, lexer_engine)
        # elif token_type == DefaultKeyword.Insert:
        #     return InsertParserFactory.new_instance(db_type, sharding_rule, lexer_engine)
        # elif token_type == DefaultKeyword.UPDATE:
        #     return UpdateParserFactory.new_instance(db_type, sharding_rule, lexer_engine)
        # elif token_type == DefaultKeyword.DELETE:
        #     return DeleteParserFactory.new_instance(db_type, sharding_rule, lexer_engine)
        # elif token_type == DefaultKeyword.CREATE:
        #     return CreateParserFactory.new_instance(db_type, sharding_rule, lexer_engine)
        # elif token_type == DefaultKeyword.ALTER:
        #     return AlterParserFactory.new_instance(db_type, sharding_rule, lexer_engine)
        # elif token_type == DefaultKeyword.DROP:
        #     return DropParserFactory.new_instance(db_type, sharding_rule, lexer_engine)
        # elif token_type == DefaultKeyword.TRUNCATE:
        #     return TruncateParserFactory.new_instance(db_type, sharding_rule, lexer_engine)
        else:
            raise SQLParsingUnsupportedException(lexer_engine.get_current_toke().token_type)
