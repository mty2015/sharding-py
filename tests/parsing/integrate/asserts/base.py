import os
from .parsers import ALL_PARSER_RESULT_SET
from tests.parsing.sql import SQLCaseType, get_supported_sql


def get_parser_result(sql_case_id):
    assert sql_case_id in ALL_PARSER_RESULT_SET, "sql_case_id not exist: {}".format(sql_case_id)
    return ALL_PARSER_RESULT_SET[sql_case_id]


class TableAssert(object):
    def __init__(self, message_helper, test_case):
        self.message_helper = message_helper
        self.test_case = test_case

    def assert_tables(self, actual, expectd):
        self.test_case.assertEqual(len(actual), len(expectd),
                                   self.message_helper.print_msg('Tables size assertion error: '))
        for e_table in expectd:
            if e_table.get('alias'):
                a_table = actual.find(e_table.get('alias'))
            else:
                a_table = actual.find(e_table.get('name'))
            self.test_case.assertTrue(a_table, self.message_helper.print_msg(
                'Table [{}] should exist'.format(e_table.get('alias') or e_table.get('name`'))))

    def assert_table(self, actual, expected):
        self.test_case.assertEqual(actual.name, expected.get('name'),
                                   self.message_helper.print_msg('Table name assertion error: '))
        self.test_case.assertEqual(actual.alias, expected.get('alias'),
                                   self.message_helper.print_msg('Table alias assertion error: '))


class ConditionAssert(object):
    def __init__(self, message_helper, test_case):
        self.message_helper = message_helper
        self.test_case = test_case

    def assert_or_condition(self, actual, expected):
        pass


class SQLStatementAssert(object):
    def __init__(self, actual, sql_case_id, sql_case_type, test_case):
        self.test_case = test_case
        self.actual = actual
        self.expected = get_parser_result(sql_case_id)
        message_helper = print_assert_message(sql_case_id, sql_case_type)
        self.table_assert = TableAssert(message_helper, test_case)
        self.condition_assert = ConditionAssert(message_helper, test_case)


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
