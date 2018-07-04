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
        self.insert_clause_parser_facade.insert_set_clause_parser.parse(result)
        self.insert_duplicate_key_update_clause_parser.parse(result)
        self._process_generated_key(result)
        return result

    def _process_generated_key(self, insert_statement):
        table_name = insert_statement.tables.get_single_table_name()
        generate_key_column = self.sharding_rule.get_generate_key_column(table_name)
        if not generate_key_column or insert_statement.generate_key_column_index != -1:
            return
        if insert_statement.get_items_tokens():
            insert_statement.get_items_tokens()[0].items.append(generate_key_column.name)
        else:
            columns_token = ItemsToken(insert_statement.columns_list_last_position)
            columns_token.items.append(generate_key_column.name)
            insert_statement.sql_tokens.append(columns_token)
