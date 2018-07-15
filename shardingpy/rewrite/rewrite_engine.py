from shardingpy.exception import UnsupportedOperationException
from shardingpy.parsing.lexer.token import DefaultKeyword
from shardingpy.parsing.parser.token import TableToken, SchemaToken, IndexToken, ItemsToken, InsertValuesToken, \
    RowCountToken, OffsetToken, OrderByToken, InsertColumnToken
from shardingpy.rewrite.placeholder import TablePlaceholder, InsertValuesPlaceholder
from shardingpy.rewrite.sqlbuilder import SQLBuilder
from shardingpy.util import sqlutil


class SQLRewriteEngine:
    def __init__(self, sharding_rule, original_sql, database_type, sql_statement, sharding_conditions, parameters):
        self.sharding_rule = sharding_rule
        self.orignial_sql = original_sql
        self.database_type = database_type
        self.sql_statement = sql_statement
        self.sharding_conditions = sharding_conditions
        self.parameters = parameters
        self.sql_tokens = sql_statement.sql_tokens[:]

    def rewrite(self, is_rewrite_limit):
        result = SQLBuilder(self.parameters)
        if not self.sql_tokens:
            return result.append_literals(self.orignial_sql)
        count = 0
        for token in self.sql_tokens:
            if count == 0:
                result.append_literals(self.orignial_sql[:token.begin_position])
            if isinstance(token, TableToken):
                self._append_table_placehodler(result, token, count, self.sql_tokens)
            elif isinstance(token, SchemaToken):
                # TODO schema
                raise UnsupportedOperationException('schema')
            elif isinstance(token, IndexToken):
                # TODO index
                raise UnsupportedOperationException('force index')
            elif isinstance(token, ItemsToken):
                self._append_items_token(result, token, count, self.sql_tokens)
            elif isinstance(token, InsertValuesToken):
                self._append_insert_values_token(result, token, count, self.sql_tokens)
            elif isinstance(token, RowCountToken):
                self._append_limit_row_count(result, token, count, self.sql_tokens, is_rewrite_limit)
            elif isinstance(token, OffsetToken):
                self._append_limit_offset_token(result, token, count, self.sql_tokens, is_rewrite_limit)
            elif isinstance(token, OrderByToken):
                self._append_order_by_token(result, count, self.sql_tokens)
            elif isinstance(token, InsertColumnToken):
                self._append_symbol_token(result, token, count, self.sql_tokens)

    def _append_table_placehodler(self, builder, table_token, count, sql_tokens):
        builder.append_placeholder(TablePlaceholder(table_token.table_name.lower()))
        begin_position = table_token.begin_position + table_token.skipped_schema_name_length + len(
            table_token.original_literals)
        self._append_rest(builder, count, sql_tokens, begin_position)

    def _append_items_token(self, builder, items_token, count, sql_tokens):
        for i in range(len(items_token.items)):
            if not (items_token.is_first_of_items_special and i == 0):
                builder.append_literals(', ')
            builder.append_literals(sqlutil.get_original_value(items_token.items[i], self.database_type))
        self._append_rest(builder, count, sql_tokens, items_token.begin_position)

    def _append_insert_values_token(self, builder, insert_values_token, count, sql_tokens):
        builder.append_placeholder(
            InsertValuesPlaceholder(insert_values_token.table_name.lower(), self.sharding_conditions))
        self._append_rest(builder, count, sql_tokens, self.sql_statement.insert_values_list_last_position)

    def _append_limit_row_count(self, builder, row_count_token, count, sql_tokens, is_rewrite_limit):
        limit = self.sql_statement.limit
        if not is_rewrite_limit:
            builder.append_literals(row_count_token.row_count)
        elif (
                self.sql_statement.group_by_items or self.sql_statement.get_aggregation_select_items()) and \
                not self.sql_statement.is_same_group_by_and_order_by_items():
            builder.append_literals(0x7fffffff)
        else:
            builder.append_literals(
                row_count_token.row_count + limit.get_offset_value if limit.is_need_rewrite_row_count() else row_count_token.row_count)
        begin_position = row_count_token.begin_position + len(str(row_count_token.row_count))
        self._append_rest(builder, count, sql_tokens, begin_position)

    def _append_limit_offset_token(self, builder, offset_token, count, sql_tokens, is_rewrite_limit):
        builder.append_literals(0 if is_rewrite_limit else offset_token.offset)
        begin_position = offset_token.begin_position + len(str(offset_token.offset))
        self._append_rest(builder, count, sql_tokens, begin_position)

    def _append_order_by_token(self, builder, count, sql_tokens):
        order_by_literals = ' {} {} '.format(DefaultKeyword.ORDER.name, DefaultKeyword.BY.name)
        i = 0
        for each in self.sql_statement.order_by_items:
            column_label = sqlutil.get_original_value(each.get_column_label(), self.database_type)
            if i == 0:
                order_by_literals += ' {} {}'.format(column_label, each.order_direction.name)
            else:
                order_by_literals += ',{} {}'.format(column_label, each.order_direction.name)
            i += 1
        order_by_literals += ' '
        builder.append_literals(order_by_literals)
        begin_position = self.sql_statement.group_by_last_position
        self._append_rest(builder, count, sql_tokens, begin_position)

    def _append_symbol_token(self, builder, insert_column_token, count, sql_tokens):
        builder.append_literals(insert_column_token.column_name)
        self._append_rest(builder, count, sql_tokens, insert_column_token.begin_position)

    def _append_rest(self, builder, count, sql_tokens, begin_position):
        end_position = len(self.orignial_sql) if len(sql_tokens) - 1 == count else sql_tokens[count + 1].begin_position
        builder.append_literals(self.orignial_sql[begin_position: end_position])

    def generate_sql(self, table_unit, sql_builder):
        return sql_builder.to_sql(table_unit, self._get_table_tokens(table_unit), self.sharding_rule)

    def _get_table_tokens(self, table_unit):
        result = dict()
        for routing_table in table_unit.routing_tables:
            logic_table_name = routing_table.logic_table_name.lower()
            result[logic_table_name] = routing_table.actual_table_name
            binding_table_rule = self.sharding_rule.find_binding_table_rule(logic_table_name)
            if binding_table_rule:
                result.update(
                    self._get_binding_table_tokens(table_unit.data_source_name, routing_table, binding_table_rule))
        return result

    def _get_binding_table_tokens(self, data_source_name, routing_table, binding_table_rule):
        result = dict()
        for each_table in self.sql_statement.tables.get_table_names():
            table_name = each_table.lower()
            if table_name != routing_table.logic_table_name and binding_table_rule.has_logic_table(table_name):
                result[table_name] = binding_table_rule.get_binding_actual_table(data_source_name, table_name,
                                                                                 routing_table.actual_table_name)
        return result
