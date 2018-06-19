import os

from shardingpy.parsing.parser.context.selectitem import AggregationSelectItem
from shardingpy.parsing.parser.token import *
from tests.parsing.sql import SQLCaseType, get_supported_sql
from .parsers import ALL_PARSER_RESULT_SET


def get_parser_result(sql_case_id):
    assert sql_case_id in ALL_PARSER_RESULT_SET, "sql_case_id not exist: {}".format(sql_case_id)
    return ALL_PARSER_RESULT_SET[sql_case_id]


class TableAssert(object):
    def __init__(self, message_helper, test_case):
        self.message_helper = message_helper
        self.test_case = test_case

    def assert_tables(self, actual, expectd):
        self.test_case.assertEqual(len(actual), len(expectd),
                                   self.message_helper('Tables size assertion error: '))
        for e_table in expectd:
            if e_table.get('alias'):
                a_table = actual.find(e_table.get('alias'))
            else:
                a_table = actual.find(e_table.get('name'))
            self.test_case.assertTrue(a_table, self.message_helper(
                'Table [{}] should exist'.format(e_table.get('alias') or e_table.get('name`'))))

    def assert_table(self, actual, expected):
        self.test_case.assertEqual(actual.name, expected.get('name'),
                                   self.message_helper('Table name assertion error: '))
        self.test_case.assertEqual(actual.alias, expected.get('alias'),
                                   self.message_helper('Table alias assertion error: '))


class ConditionAssert(object):
    def __init__(self, message_helper, test_case):
        self.message_helper = message_helper
        self.test_case = test_case

    def assert_or_condition(self, actual, expected):
        """
        expected: [[{'table_name': 'xx', 'column_name': 'xxx', 'operator': 'EQUAL', 'values': [(0, 1)]}, ... ], ... ]
        """
        self.test_case.assertEqual(len(actual.and_conditions), len(expected),
                                   self.message_helper('Or condition size assertion error: '))
        count = 0
        for each in actual.and_conditions:
            self.assert_and_condition(each, expected[count])
            count += 1

    def assert_and_condition(self, actual, expected):
        self.test_case.assertEqual(len(actual.conditions), expected)
        count = 0
        for each in actual.conditions:
            self.assert_condition(each, expected[count])
            count += 1

    def assert_condition(self, actual, expected):
        self.test_case.assertEqual(actual.column.table_name.upper(), expected['table_name'].upper(),
                                   self.message_helper('Condition table name assertion error: '))
        self.test_case.assertEqual(actual.column.name.upper(), expected['column_name'].upper(),
                                   self.message_helper('Condition column name assertion error: '))
        self.test_case.assertEqual(actual.column.operator.name, expected['operator'].upper(),
                                   self.message_helper('Condition column name assertion error: '))
        count = 0
        for index, value in expected['values']:
            if actual['_values']:
                self.test_case.assertEqual(actual['_values'][count], value,
                                           self.message_helper('Condition parameter value assertion error: '))
            elif actual['_position_index_map']:
                self.test_case.assertEqual(actual.get(count), index,
                                           self.message_helper('Condition parameter index assertion error: '))
            count += 1


class TableTokenAssert:
    def __init__(self, message_helper, test_case):
        self.message_helper = message_helper
        self.test_case = test_case

    def assert_table_tokens(self, actual, expected):
        table_tokens = [token for token in actual if type(token) == TableToken]
        self.test_case.assertEqual(len(table_tokens), len(expected.get('table_tokens', [])),
                                   self.message_helper('Table tokens size error: '))
        for i in range(len(table_tokens)):
            self.assert_table_token(table_tokens[i], expected.get('table_tokens')[i])

    def assert_table_token(self, actual, expected):
        self.test_case.assertEqual(actual.begin_position, expected.get('begin_position'),
                                   self.message_helper('Table tokens begin position assertion error: '))
        self.test_case.assertEqual(actual.original_literals, expected.get('original_literals'),
                                   self.message_helper('Table tokens original literals assertion error: '))


class IndexTokenAssert:
    def __init__(self, message_helper, test_case):
        self.message_helper = message_helper
        self.test_case = test_case

    def assert_index_token(self, actual, expected):
        index_tokens = [token for token in actual if type(token) == IndexToken]
        if index_tokens:
            self._assert_index_token(index_tokens[0], expected.get('index_token', {}))
        else:
            self.test_case.assertIsNone(expected.get('index_token'),
                                        self.message_helper('Index token should not exist: '))

    def _assert_index_token(self, actual, expected):
        self.test_case.assertEqual(actual.begin_position, expected.get('begin_position'),
                                   self.message_helper('Index token begin position assertion error: '))
        self.test_case.assertEqual(actual.original_literals, expected.get('original_literals'),
                                   self.message_helper('Index token original literals assertion error: '))
        self.test_case.assertEqual(actual.table_name, expected.get('table_name'),
                                   self.message_helper('Index token table name assertion error: '))


class ItemsTokenAssert:
    def __init__(self, message_helper, test_case):
        self.message_helper = message_helper
        self.test_case = test_case

    def assert_items_token(self, actual, expected):
        items_tokens = [token for token in actual if type(token) == ItemsToken]
        if items_tokens:
            items_token = items_tokens[0]
            self.test_case.assertEqual(items_token.begin_position,
                                       expected.get('items_token', {}).get('begin_position'))
            self.test_case.assertEqual(items_token.items, expected.get('items_token', {}).get('items'))
        else:
            self.test_case.assertFalse(expected.get('items_token'),
                                       self.message_helper('Items token should not exist: '))


class GeneratedKeyTokenAssert:
    def __init__(self, message_helper, test_case, sql_case_type):
        self.message_helper = message_helper
        self.test_case = test_case
        self.sql_case_type = sql_case_type

    def assert_generated_key_token(self, actual, expected):
        generated_key_tokens = [token for token in actual if type(token) == GeneratedKeyToken]
        if generated_key_tokens:
            generated_key_token = generated_key_tokens[0]
            if self.sql_case_type == SQLCaseType.Placeholder:
                self.test_case.assertEqual(generated_key_token.begin_position,
                                           expected.get('generated_key_token', {}).get('placeholder_begin_position'),
                                           self.message_helper('Generated key token begin position assertion error: '))
            else:
                self.test_case.assertEqual(generated_key_token.begin_position,
                                           expected.get('generated_key_token', {}).get('literal_begin_position'),
                                           self.message_helper('Generated key token begin position assertion error: '))

        else:
            self.test_case.assertFalse(expected.get('Generated key token should not exist: '))


class InsertValuesTokenAssert:
    def __init__(self, message_helper, test_case):
        self.message_helper = message_helper
        self.test_case = test_case

    def assert_insert_values_token(self, actual, expected):
        insert_values_tokens = [token for token in actual if type(token) == InsertValuesToken]
        if insert_values_tokens:
            insert_value_token = insert_values_tokens[0]
            self.test_case.assertEqual(insert_value_token.begin_position,
                                       expected.get('insert_values_token', {}).get('begin_position'),
                                       self.message_helper('Insert values token begin position assertion error: '))
            self.test_case.assertEqual(insert_value_token.table_name,
                                       expected.get('insert_values_token', {}).get('table_name'))
        else:
            self.test_case.assertFalse(expected.get('insert_values_token'),
                                       self.message_helper('Insert values token should not exist: '))


class OrderByTokenAssert:
    def __init__(self, message_helper, test_case, sql_case_type):
        self.message_helper = message_helper
        self.test_case = test_case
        self.sql_case_type = sql_case_type

    def assert_order_by_token(self, actual, expected):
        order_by_tokens = [token for token in actual if type(token) == OrderByToken]
        if order_by_tokens:
            order_by_token = order_by_tokens[0]
            if self.sql_case_type == SQLCaseType.Placeholder:
                self.test_case.assertEqual(order_by_token.begin_position,
                                           expected.get('order_by_token', {}).get('placeholder_begin_position'),
                                           self.message_helper('Order by token begin position assertion error: '))
            else:
                self.test_case.assertEqual(order_by_token.begin_position,
                                           expected.get('order_by_token', {}).get('literal_begin_position'),
                                           self.message_helper('Order by token  begin position assertion error: '))
        else:
            self.test_case.assertFalse(expected.get('order_by_token'),
                                       self.message_helper('Order by token should not exist: '))


class OffsetTokenAssert:
    def __init__(self, message_helper, test_case, sql_case_type):
        self.message_helper = message_helper
        self.test_case = test_case
        self.sql_case_type = sql_case_type

    def assert_offset_token(self, actual, expected):
        offset_tokens = [token for token in actual if type(token) == OffsetToken]
        if self.sql_case_type == SQLCaseType.Placeholder:
            self.test_case.assertFalse(offset_tokens, self.message_helper('Offset token should not exist: '))
            return
        if offset_tokens:
            offset_token = offset_tokens[0]
            self.test_case.assertEqual(offset_token.begin_position,
                                       expected.get('offset_token', {}).get('begin_position'),
                                       self.message_helper('offset token begin position assertion error: '))
            self.test_case.assertEqual(offset_token.offset,
                                       expected.get('offset_token', {}).get('offset'),
                                       self.message_helper('Offset token offset assertion error: '))
        else:
            self.test_case.assertFalse(expected.get('offset_token'),
                                       self.message_helper('Offset token should not exist: '))


class RowCountTokenAssert:
    def __init__(self, message_helper, test_case, sql_case_type):
        self.message_helper = message_helper
        self.test_case = test_case
        self.sql_case_type = sql_case_type

    def assert_row_count_token(self, actual, expected):
        row_count_tokens = [token for token in actual if type(token) == RowCountToken]
        if self.sql_case_type == SQLCaseType.Placeholder:
            self.test_case.assertFalse(row_count_tokens, self.message_helper('row count token should not exist: '))
            return
        if row_count_tokens:
            row_count_token = row_count_tokens[0]
            self.test_case.assertEqual(row_count_token.begin_position,
                                       expected.get('row_count_token', {}).get('begin_position'),
                                       self.message_helper('row count token begin position assertion error: '))
            self.test_case.assertEqual(row_count_token.row_count,
                                       expected.get('row_count_token', {}).get('row_count'),
                                       self.message_helper('row count token offset assertion error: '))
        else:
            self.test_case.assertFalse(expected.get('row_count_token'),
                                       self.message_helper('row count token should not exist: '))


class TokenAssert:
    def __init__(self, message_helper, test_case, sql_case_type):
        self.message_helper = message_helper
        self.test_case = test_case
        self.sql_case_type = sql_case_type

        self.table_token_assert = TableTokenAssert(message_helper, test_case)
        self.index_token_assert = IndexTokenAssert(message_helper, test_case)
        self.items_token_assert = ItemsTokenAssert(message_helper, test_case)
        self.generated_key_token_assert = GeneratedKeyTokenAssert(message_helper, test_case, sql_case_type)
        self.insert_values_token_assert = InsertValuesTokenAssert(message_helper, test_case)
        self.order_by_token_assert = OrderByTokenAssert(message_helper, test_case, sql_case_type)
        self.offset_token_assert = OffsetTokenAssert(message_helper, test_case, sql_case_type)
        self.row_count_token_assert = RowCountTokenAssert(message_helper, test_case, sql_case_type)

    def assert_tokens(self, actual, expected):
        self.table_token_assert.assert_table_tokens(actual, expected)
        self.index_token_assert.assert_index_token(actual, expected)
        self.items_token_assert.assert_items_token(actual, expected)
        self.generated_key_token_assert.assert_generated_key_token(actual, expected)
        self.insert_values_token_assert.assert_insert_values_token(actual, expected)
        self.order_by_token_assert.assert_order_by_token(actual, expected)
        self.offset_token_assert.assert_offset_token(actual, expected)
        self.row_count_token_assert.assert_row_count_token(actual, expected)


class IndexAssert:
    def __init__(self, message_helper, test_case, sql_case_type):
        self.message_helper = message_helper
        self.test_case = test_case
        self.sql_case_type = sql_case_type

    def assert_parameters_index(self, actual, expected):
        if self.sql_case_type == SQLCaseType.Placeholder:
            self.test_case.assertEqual(actual, expected, self.message_helper('Parameters index assertion error: '))
        else:
            self.test_case.assertEqual(actual, 0, self.message_helper('Parameters index assertion error: '))


class ItemAssert:
    def __init__(self, message_helper, test_case):
        self.message_helper = message_helper
        self.test_case = test_case

    def assert_items(self, actual, expected):
        self.assert_aggregation_select_items(actual, expected)

    def assert_aggregation_select_items(self, actual, expected):
        aggregation_select_items = [each for each in actual if type(each) == AggregationSelectItem]
        self.test_case.assertEqual(len(aggregation_select_items), len(expected),
                                   self.message_helper('aggregation select items size error: '))
        for each1, each2 in zip(actual, expected):
            self.assert_aggregation_select_item(each1, each2)

    def assert_aggregation_select_item(self, actual, expected):
        self.test_case.assertEqual(actual.aggregation_tyoe.name, expected.get('type'),
                                   self.message_helper('Aggregation select item aggregation type assertion error: '))
        self.test_case.assertEqual(actual.inner_expression, expected.get('inner_expression'),
                                   self.message_helper('Aggregation select item inner expression assertion error: '))
        self.test_case.assertEqual(actual.alias, expected.get('alias'),
                                   self.message_helper('Aggregation select item alias assertion error: '))
        self.test_case.assertEqual(actual.index, expected.get('index'),
                                   self.message_helper('Aggregation select item index assertion error: '))
        for each1, each2 in zip(actual.derived_aggregation_select_items, expected.get('derived_columns', [])):
            self.assert_aggregation_select_item(each1, each2)


class SQLStatementAssert(object):
    def __init__(self, actual, sql_case_id, sql_case_type, test_case):
        self.test_case = test_case
        self.actual = actual
        self.expected = get_parser_result(sql_case_id)
        message_helper = print_assert_message(sql_case_id, sql_case_type)
        self.table_assert = TableAssert(message_helper, test_case)
        self.condition_assert = ConditionAssert(message_helper, test_case)
        self.token_assert = TokenAssert(message_helper, test_case, sql_case_type)
        self.index_assert = IndexAssert(message_helper, test_case, sql_case_type)
        self.item_assert = ItemAssert(message_helper, test_case)


def print_assert_message(sql_case_id, sql_case_type):
    def print_msg(assert_msg):
        result = 'SQL Case ID: ' + sql_case_id + os.linesep
        result += 'SQL      :'
        sql = get_supported_sql(sql_case_id, sql_case_type, get_parser_result(sql_case_id).get('parameters'))
        result += sql
        if sql_case_type == SQLCaseType.Placeholder:
            result += os.linesep
            result += 'SQL Params   :' + get_parser_result(sql_case_id).get('parameters')
            result += os.linesep
        result += os.linesep
        result += assert_msg
        result += os.linesep
        return result

    return print_msg
