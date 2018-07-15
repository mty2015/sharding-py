import unittest

from shardingpy.api.config.base import ShardingRuleConfiguration, TableRuleConfiguration
from shardingpy.exception import ShardingException
from shardingpy.rewrite.placeholder import TablePlaceholder, IndexPlaceholder, SchemaPlaceholder
from shardingpy.rewrite.sqlbuilder import SQLBuilder
from shardingpy.rule.base import ShardingRule


class SQLBuilderTest(unittest.TestCase):

    def test_append_literals_only(self):
        builder = SQLBuilder()
        builder.append_literals('SELECT ')
        builder.append_literals('table_x')
        builder.append_literals('.id')
        builder.append_literals(' FROM ')
        builder.append_literals('table_x')
        self.assertEqual(builder.to_sql(None, dict(), None).sql, 'SELECT table_x.id FROM table_x')

    def test_append_table_without_table_token(self):
        builder = SQLBuilder()
        builder.append_literals('SELECT ')
        builder.append_literals('table_x')
        builder.append_literals('.id')
        builder.append_literals(' FROM ')
        builder.append_placeholder(TablePlaceholder('table_x'))
        self.assertEqual(builder.to_sql(None, dict(), None).sql, 'SELECT table_x.id FROM table_x')

    def test_append_table_with_table_token(self):
        builder = SQLBuilder()
        builder.append_literals('SELECT ')
        builder.append_placeholder(TablePlaceholder('table_x'))
        builder.append_literals('.id')
        builder.append_literals(' FROM ')
        builder.append_placeholder(TablePlaceholder('table_x'))
        self.assertEqual(builder.to_sql(None, {'table_x': 'table_x_1'}, None).sql, 'SELECT table_x_1.id FROM table_x_1')

    def test_index_placeholder_append_table_without_table_token(self):
        builder = SQLBuilder()
        builder.append_literals('CREATE INDEX ')
        builder.append_placeholder(IndexPlaceholder('index_name', 'table_x'))
        builder.append_literals(' ON ')
        builder.append_placeholder(TablePlaceholder('table_x'))
        builder.append_literals(" ('column')")
        self.assertEqual(builder.to_sql(None, dict(), None).sql, "CREATE INDEX index_name ON table_x ('column')")

    def test_index_placeholder_append_table_with_table_token(self):
        builder = SQLBuilder()
        builder.append_literals('CREATE INDEX ')
        builder.append_placeholder(IndexPlaceholder('index_name', 'table_x'))
        builder.append_literals(' ON ')
        builder.append_placeholder(TablePlaceholder('table_x'))
        builder.append_literals(" ('column')")
        self.assertEqual(builder.to_sql(None, {'table_x': 'table_x_1'}, None).sql,
                         "CREATE INDEX index_name_table_x_1 ON table_x_1 ('column')")

    def test_schema_placeholder_append_table_without_token(self):
        builder = SQLBuilder()
        builder.append_literals('SHOW ')
        builder.append_literals('CREATE TABLE ')
        builder.append_placeholder(TablePlaceholder('table_x'))
        builder.append_literals(' ON ')
        builder.append_placeholder(SchemaPlaceholder('dx', 'table_x'))
        try:
            builder.to_sql(None, dict(), self._create_sharding_rule()).sql
            assert False, 'must raise ShardingException'
        except ShardingException:
            pass

    def test_schema_placeholder_append_table_with_token(self):
        builder = SQLBuilder()
        builder.append_literals('SHOW ')
        builder.append_literals('CREATE TABLE ')
        builder.append_placeholder(TablePlaceholder('table_0'))
        builder.append_literals(' ON ')
        builder.append_placeholder(SchemaPlaceholder('ds', 'table_0'))
        self.assertEqual(builder.to_sql(None, {'table_0': 'table_1'}, self._create_sharding_rule()).sql,
                         "SHOW CREATE TABLE table_1 ON ds0")

    def _create_sharding_rule(self):
        config = ShardingRuleConfiguration()
        table_rule_config = TableRuleConfiguration()
        table_rule_config.logic_table = 'LOGIC_TABLE'
        table_rule_config.actual_data_nodes = ['ds0.table_0', 'ds0.table_1', 'ds1.table_0', 'ds1.table_1']
        config.table_rule_configs.append(table_rule_config)
        return ShardingRule(config, ['ds0', 'ds1'])
