import unittest
from shardingpy.api.config.base import load_sharding_rule_config_from_dict
from shardingpy.constant import DatabaseType
from shardingpy.optimizer.condition import ShardingConditions
from shardingpy.optimizer.insert_optimizer import InsertShardingCondition
from shardingpy.parsing.parser.sql.dml.insert import InsertStatement
from shardingpy.parsing.parser.sql.dql.select import SelectStatement
from shardingpy.parsing.parser.token import TableToken, ItemsToken, InsertValuesToken, InsertColumnToken
from shardingpy.rewrite.rewrite_engine import SQLRewriteEngine
from shardingpy.routing.types.base import TableUnit, RoutingTable
from shardingpy.rule.base import ShardingRule, DataNode
from . import rewrite_rule


class SQLRewriteEngineTest(unittest.TestCase):
    def setUp(self):
        sharding_rule_config = load_sharding_rule_config_from_dict(rewrite_rule.sharding_rule_config['sharding_rule'])
        self.sharding_rule = ShardingRule(sharding_rule_config,
                                          rewrite_rule.sharding_rule_config['data_sources'].keys())
        self.select_statement = SelectStatement()
        self.insert_statement = InsertStatement()
        self.table_tokens = {'table_x': 'table_1'}

    def test_rewrite_without_change(self):
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, 'SELECT table_y.id FROM table_y WHERE table_y.id=?',
                                          DatabaseType.MySQL, self.select_statement, None, [1])
        self.assertEqual(rewrite_engine.rewrite(True).to_sql(None, self.table_tokens, None).sql,
                         'SELECT table_y.id FROM table_y WHERE table_y.id=?')

    def test_rewrite_for_table_name(self):
        self.select_statement.sql_tokens.append(TableToken(7, 0, 'table_x'))
        self.select_statement.sql_tokens.append(TableToken(31, 0, 'table_x'))
        self.select_statement.sql_tokens.append(TableToken(47, 0, 'table_x'))
        sql = 'SELECT table_x.id, x.name FROM table_x x WHERE table_x.id=? AND x.name=?'
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, sql, DatabaseType.MySQL, self.select_statement, None,
                                          [1, 'x'])
        rewrite_sql = 'SELECT table_1.id, x.name FROM table_1 x WHERE table_1.id=? AND x.name=?'
        self.assertEqual(rewrite_engine.rewrite(True).to_sql(None, self.table_tokens, None).sql, rewrite_sql)

    def test_rewrite_for_order_by_and_group_by_by_derived_columns(self):
        self.select_statement.sql_tokens.append(TableToken(18, 0, 'table_x'))
        items_token = ItemsToken(12)
        items_token.items.extend(['x.id as GROUP_BY_DERIVED_0', 'x.name as ORDER_BY_DERIVED_0'])
        self.select_statement.sql_tokens.append(items_token)
        sql = 'SELECT x.age FROM table_x x GROUP BY x.id ORDER BY x.name'
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, sql, DatabaseType.MySQL, self.select_statement, None,
                                          [])
        rewrite_sql = 'SELECT x.age, x.id as GROUP_BY_DERIVED_0, x.name as ORDER_BY_DERIVED_0 FROM table_1 x GROUP BY x.id ORDER BY x.name'
        self.assertEqual(rewrite_engine.rewrite(True).to_sql(None, self.table_tokens, None).sql, rewrite_sql)

    def test_rewrite_for_aggregation_derived_columns(self):
        self.select_statement.sql_tokens.append(TableToken(23, 0, 'table_x'))
        items_token = ItemsToken(17)
        items_token.items.extend(['COUNT(x.age) as AVG_DERIVED_COUNT_0', 'SUM(x.age) as AVG_DERIVED_SUM_0'])
        self.select_statement.sql_tokens.append(items_token)
        sql = 'SELECT AVG(x.age) FROM table_x x'
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, sql, DatabaseType.MySQL, self.select_statement, None,
                                          [])
        rewrite_sql = 'SELECT AVG(x.age), COUNT(x.age) as AVG_DERIVED_COUNT_0, SUM(x.age) as AVG_DERIVED_SUM_0 FROM table_1 x'
        self.assertEqual(rewrite_engine.rewrite(True).to_sql(None, self.table_tokens, None).sql, rewrite_sql)

    def test_rewrite_auto_generated_key_column(self):
        parameters = ['x', 1]
        self.insert_statement.parameters_index = 2
        self.insert_statement.insert_values_list_last_position = 45
        self.insert_statement.sql_tokens.append(TableToken(12, 0, 'table_x'))
        items_token = ItemsToken(30)
        items_token.items.append('id')
        self.insert_statement.sql_tokens.append(items_token)
        self.insert_statement.sql_tokens.append(InsertValuesToken(39, 'table_x'))
        sharding_condition = InsertShardingCondition('(?, ?, ?)', parameters)
        sharding_condition.data_nodes.append(DataNode('db0.table_1'))
        table_unit = TableUnit('db0')
        table_unit.routing_tables.append(RoutingTable('table_x', 'table_1'))
        sql = 'INSERT INTO table_x (name, age) VALUES (?, ?)'
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, sql, DatabaseType.MySQL, self.insert_statement,
                                          ShardingConditions([sharding_condition]), parameters)
        rewrite_sql = 'INSERT INTO table_1 (name, age, id) VALUES (?, ?, ?)'
        self.assertEqual(rewrite_engine.rewrite(True).to_sql(table_unit, self.table_tokens, None).sql, rewrite_sql)

    def test_rewrite_for_auto_generated_key_column_without_columns_with_parameter(self):
        parameters = ['Bill']
        self.insert_statement.parameters_index = 1
        self.insert_statement.insert_values_list_last_position = 32
        self.insert_statement.sql_tokens.append(TableToken(12, 0, '`table_x`'))
        self.insert_statement.generate_key_column_index = 0
        self.insert_statement.sql_tokens.append(InsertColumnToken(21, '('))
        items_token = ItemsToken(21)
        items_token.is_first_of_items_special = True
        items_token.items.append('name')
        items_token.items.append('id')
        self.insert_statement.sql_tokens.append(items_token)
        self.insert_statement.sql_tokens.append(InsertColumnToken(21, ')'))
        self.insert_statement.sql_tokens.append(InsertValuesToken(29, 'table_x'))
        sharding_condition = InsertShardingCondition('(?, ?)', parameters)
        sharding_condition.data_nodes.append(DataNode('db0.table_1'))
        table_unit = TableUnit('db0')
        table_unit.routing_tables.append(RoutingTable('table_x', 'table_1'))
        sql = 'INSERT INTO `table_x` VALUES (?)'
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, sql, DatabaseType.MySQL, self.insert_statement,
                                          ShardingConditions([sharding_condition]), parameters)
        rewrite_sql = 'INSERT INTO table_1(name, id) VALUES (?, ?)'
        self.assertEqual(rewrite_engine.rewrite(True).to_sql(table_unit, self.table_tokens, None).sql, rewrite_sql)

    def test_rewrite_for_auto_generated_key_column_without_columns_without_parameter(self):
        self.insert_statement.insert_values_list_last_position = 33
        self.insert_statement.sql_tokens.append(TableToken(12, 0, '`table_x`'))
        self.insert_statement.generate_key_column_index = 0
        self.insert_statement.sql_tokens.append(InsertColumnToken(21, '('))
        items_token = ItemsToken(21)
        items_token.is_first_of_items_special = True
        items_token.items.append('name')
        items_token.items.append('id')
        self.insert_statement.sql_tokens.append(items_token)
        self.insert_statement.sql_tokens.append(InsertColumnToken(21, ')'))
        self.insert_statement.sql_tokens.append(InsertValuesToken(29, 'table_x'))
        sharding_condition = InsertShardingCondition('(10, 1)', [])
        sharding_condition.data_nodes.append(DataNode('db0.table_1'))
        table_unit = TableUnit('db0')
        table_unit.routing_tables.append(RoutingTable('table_x', 'table_1'))
        sql = 'INSERT INTO `table_x` VALUES (10)'
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, sql, DatabaseType.MySQL, self.insert_statement,
                                          ShardingConditions([sharding_condition]), [])
        rewrite_sql = 'INSERT INTO table_1(name, id) VALUES (10, 1)'
        self.assertEqual(rewrite_engine.rewrite(True).to_sql(table_unit, self.table_tokens, None).sql, rewrite_sql)

    def test_rewrite_column_without_columns_without_parameters(self):
        self.insert_statement.insert_values_list_last_position = 36
        self.insert_statement.sql_tokens.append(TableToken(12, 0, '`table_x`'))
        self.insert_statement.generate_key_column_index = 0
        self.insert_statement.sql_tokens.append(InsertColumnToken(21, '('))
        items_token = ItemsToken(21)
        items_token.is_first_of_items_special = True
        items_token.items.append('name')
        items_token.items.append('id')
        self.insert_statement.sql_tokens.append(items_token)
        self.insert_statement.sql_tokens.append(InsertColumnToken(21, ')'))
        self.insert_statement.sql_tokens.append(InsertValuesToken(29, 'table_x'))
        sharding_condition = InsertShardingCondition('(10, 1)', [])
        sharding_condition.data_nodes.append(DataNode('db0.table_1'))
        table_unit = TableUnit('db0')
        table_unit.routing_tables.append(RoutingTable('table_x', 'table_1'))
        sql = 'INSERT INTO `table_x` VALUES (10, 1)'
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, sql, DatabaseType.MySQL, self.insert_statement,
                                          ShardingConditions([sharding_condition]), [])
        rewrite_sql = 'INSERT INTO table_1(name, id) VALUES (10, 1)'
        self.assertEqual(rewrite_engine.rewrite(True).to_sql(table_unit, self.table_tokens, None).sql, rewrite_sql)

    def test_rewrite_column_without_columns_with_parameters(self):
        parameters = ['x', 1]
        self.insert_statement.insert_values_list_last_position = 35
        self.insert_statement.sql_tokens.append(TableToken(12, 0, '`table_x`'))
        self.insert_statement.generate_key_column_index = 0
        self.insert_statement.sql_tokens.append(InsertColumnToken(21, '('))
        items_token = ItemsToken(21)
        items_token.is_first_of_items_special = True
        items_token.items.append('name')
        items_token.items.append('id')
        self.insert_statement.sql_tokens.append(items_token)
        self.insert_statement.sql_tokens.append(InsertColumnToken(21, ')'))
        self.insert_statement.sql_tokens.append(InsertValuesToken(29, 'table_x'))
        sharding_condition = InsertShardingCondition('(?, ?)', parameters)
        sharding_condition.data_nodes.append(DataNode('db0.table_1'))
        table_unit = TableUnit('db0')
        table_unit.routing_tables.append(RoutingTable('table_x', 'table_1'))
        sql = 'INSERT INTO `table_x` VALUES (?, ?)'
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, sql, DatabaseType.MySQL, self.insert_statement,
                                          ShardingConditions([sharding_condition]), [])
        rewrite_sql = 'INSERT INTO table_1(name, id) VALUES (?, ?)'
        self.assertEqual(rewrite_engine.rewrite(True).to_sql(table_unit, self.table_tokens, None).sql, rewrite_sql)
