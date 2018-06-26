from shardingpy.exception import SQLParsingUnsupportedException
from shardingpy.parsing.lexer.token import DefaultKeyword
from shardingpy.parsing.parser.sql.dql import select


class SQLParserFactory:
    @classmethod
    def new_instance(cls, db_type, token_type, sharding_rule, lexer_engine):
        if token_type is DefaultKeyword.SELECT:
            return select.new_select_parser(db_type, sharding_rule, lexer_engine)
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