from shardingpy.parsing.parser.parser_engine import SQLParsingEngine
from shardingpy.parsing.parser.sql.dml.insert import InsertStatement
from shardingpy.routing.router.sharding.base import ShardingRouter, GeneratedKey


class ParsingSQLRouter(ShardingRouter):
    def __init__(self, sharding_rule, sharding_meta_data, database_type, show_sql):
        self.sharding_rule = sharding_rule
        self.sharding_meta_data = sharding_meta_data
        self.database_type = database_type
        self.show_sql = show_sql
        self.generated_keys = list()

    def parse(self, logic_sql, user_cache):
        return SQLParsingEngine(self.database_type, logic_sql, self.sharding_rule, self.sharding_meta_data).parse()

    def route(self, logic_sql, parameters, sql_statement):
        generated_key = None
        if isinstance(sql_statement, InsertStatement):
            generated_key = self._get_generate_key(sql_statement, parameters)

    def _get_generate_key(self, insert_statement, parameters):
        generated_key = None
        if insert_statement.generate_key_column_index != -1:
            for each in insert_statement.generated_key_conditions:
                if generated_key is None:
                    generated_key = GeneratedKey(each.column)
                if each.index == -1:
                    generated_key.generated_keys.append(each.value)
                else:
                    generated_key.generated_keys.append(parameters[each.index])
            return generated_key

        logic_table_name = insert_statement.tables.get_single_table_name()
        table_rule = self.sharding_rule.try_find_table_rule_by_logic_table(logic_table_name)
        if not table_rule:
            return None
        generate_key_column = self.sharding_rule.get_generate_key_column(logic_table_name)
        if generate_key_column:
            generated_key = GeneratedKey(generate_key_column)
            for i in range(len(insert_statement.insert_values.insert_values)):
                generated_key.generated_keys.append(self.sharding_rule.generate_key(logic_table_name))

        return generated_key

