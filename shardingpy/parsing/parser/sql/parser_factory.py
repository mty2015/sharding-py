from shardingpy.exception import SQLParsingUnsupportedException
from shardingpy.parsing.lexer.token import DefaultKeyword
from shardingpy.parsing.parser.sql.dml import insert
from shardingpy.parsing.parser.sql.dql import select


class SQLParserFactory:
    @staticmethod
    def new_instance(db_type, token_type, sharding_rule, lexer_engine, sharding_meta_data):
        if token_type is DefaultKeyword.SELECT:
            return select.new_select_parser(db_type, sharding_rule, lexer_engine)
        elif token_type is DefaultKeyword.INSERT:
            return SQLParserFactory._get_dml_parser(db_type, token_type, sharding_rule, lexer_engine,
                                                    sharding_meta_data)
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
            raise SQLParsingUnsupportedException(lexer_engine.get_current_token().token_type)

    @staticmethod
    def _get_dml_parser(db_type, token_type, sharding_rule, lexer_engine, sharding_meta_data):
        if token_type is DefaultKeyword.INSERT:
            return insert.new_insert_parser(db_type, sharding_rule, lexer_engine, sharding_meta_data)
