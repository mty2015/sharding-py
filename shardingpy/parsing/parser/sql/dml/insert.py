from shardingpy.constant import SQLType
from shardingpy.exception import UnsupportedOperationException
from shardingpy.parsing.lexer.token import DefaultKeyword, Symbol
from shardingpy.parsing.parser.context.insertvalue import InsertValues
from shardingpy.parsing.parser.sql import SQLStatement
from shardingpy.parsing.parser.token import ItemsToken


class InsertStatement(SQLStatement):
    def __init__(self):
        super().__init__(SQLType.DML)
        self.columns = list()
        self.generated_key_conditions = list()
        self.insert_values = InsertValues()
        self.columns_list_last_position = 0
        self.generate_key_column_index = -1
        self.insert_values_list_last_position = 0

    def get_items_tokens(self):
        return [i for i in self.sql_tokens if isinstance(i, ItemsToken)]


class AbstractInsertParser:
    def __init__(self, sharding_rule, sharding_meta_data, lexer_engine, insert_clause_parser_facade):
        self.sharding_rule = sharding_rule
        self.sharding_meta_data = sharding_meta_data
        self.lexer_engine = lexer_engine
        self.insert_clause_parser_facade = insert_clause_parser_facade

    def parse(self):
        self.lexer_engine.next_token()
        result = InsertStatement()
        self.insert_clause_parser_facade.insert_into_clause_parser.parse(result)
        self.insert_clause_parser_facade.insert_columns_clause_parser(result, self.sharding_meta_data)
        if self.lexer_engine.equal_any(DefaultKeyword.SELECT, Symbol.LEFT_PAREN):
            raise UnsupportedOperationException('Cannot INSERT SELECT')
        self.insert_clause_parser_facade.insert_values_clause_parser.parse(result)
