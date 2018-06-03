import unittest

from shardingpy.constant import DatabaseType
from shardingpy.parsing.parser.parser_engine import SQLParsingEngine
from . import asserts
from .. import sql as sql_cases_loader


class IntegrateSupportedSQLParsingTestCase(unittest.TestCase):

    def test_supported_sqls(self):
        for sql_case_id, database_type, sql_case_type in sql_cases_loader.get_supported_sql_test_parameters(
                [DatabaseType.MySQL, DatabaseType.H2]):
            self.assert_supported_sql(sql_case_id, database_type, sql_case_type)

    def assert_supported_sql(self, sql_case_id, database_type, sql_case_type):
        print("test: {} - {} - {}".format(sql_case_id, database_type.name, sql_case_type.name))
        sql = sql_cases_loader.get_supported_sql(sql_case_id, sql_case_type,
                                                 asserts.get_parser_result(sql_case_id).get('parameters'))
        SQLParsingEngine(database_type, sql, self._get_sharding_rule())

    def _get_sharding_rule(self):
        pass
