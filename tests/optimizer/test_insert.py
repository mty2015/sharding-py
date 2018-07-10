import unittest

from shardingpy.api.config.base import load_sharding_rule_config_from_dict
from shardingpy.constant import ShardingOperator
from shardingpy.optimizer.insert_optimizer import InsertOptimizeEngine
from shardingpy.parsing.parser.context.condition import AndCondition, Condition, Column
from shardingpy.parsing.parser.context.insertvalue import InsertValue
from shardingpy.parsing.parser.context.table import Table
from shardingpy.parsing.parser.expressionparser import SQLPlaceholderExpression
from shardingpy.parsing.parser.sql.dml.insert import InsertStatement
from shardingpy.parsing.parser.token import TableToken, InsertValuesToken
from shardingpy.routing.router.sharding.base import GeneratedKey
from shardingpy.rule.base import ShardingRule
from . import optimizer_rule


class InsertOptimizeEngineTest(unittest.TestCase):

    def setUp(self):
        sharding_rule_config = load_sharding_rule_config_from_dict(optimizer_rule.sharding_rule_config['sharding_rule'])
        self.sharding_rule = ShardingRule(sharding_rule_config,
                                          optimizer_rule.sharding_rule_config['data_sources'].keys())

        self.insert_statement = insert_statement = InsertStatement()
        insert_statement.tables.add(Table('t_order', None))
        insert_statement.parameters_index = 4
        insert_statement.insert_values_list_last_position = 45
        insert_statement.sql_tokens.append(TableToken(12, 0, 't_order'))
        insert_statement.sql_tokens.append(InsertValuesToken(39, 't_order'))

        and_condition1 = AndCondition()
        and_condition1.conditions.append(
            Condition(Column('user_id', 't_order'), ShardingOperator.EQUAL, SQLPlaceholderExpression(0)))
        insert_statement.conditions.or_condition.and_conditions.append(and_condition1)

        and_condition2 = AndCondition()
        and_condition2.conditions.append(
            Condition(Column('user_id', 't_order'), ShardingOperator.EQUAL, SQLPlaceholderExpression(2)))
        insert_statement.conditions.or_condition.and_conditions.append(and_condition2)

        insert_statement.insert_values.insert_values.append(InsertValue('(?, ?)', 2))
        insert_statement.insert_values.insert_values.append(InsertValue('(?, ?)', 2))

        self.parameters = [10, 'init', 11, 'init']

    def test_optimize_with_generated_key(self):
        generated_key = GeneratedKey(Column('order_id', 't_order'))
        generated_key.generated_keys = [1, 2]
        actual = InsertOptimizeEngine(self.sharding_rule, self.insert_statement, self.parameters,
                                      generated_key).optimize()
        self.assertFalse(actual.is_always_false())
        self.assertEqual(len(actual.sharding_conditions), 2)
        self.assertEqual(len(actual.sharding_conditions[0].parameters), 3)
        self.assertEqual(len(actual.sharding_conditions[1].parameters), 3)
        self.assertEqual(actual.sharding_conditions[0].parameters, [10, 'init', 1])
        self.assertEqual(actual.sharding_conditions[1].parameters, [11, 'init', 2])
        self.assertEqual(actual.sharding_conditions[0].insert_value_expression, '(?, ?, ?)')
        self.assertEqual(actual.sharding_conditions[1].insert_value_expression, '(?, ?, ?)')
        self.assertEqual(len(actual.sharding_conditions[0].sharding_values), 2)
        self.assertEqual(len(actual.sharding_conditions[1].sharding_values), 2)
        self._assert_sharding_value(actual.sharding_conditions[0].sharding_values[0], 1)
        self._assert_sharding_value(actual.sharding_conditions[0].sharding_values[1], 10)
        self._assert_sharding_value(actual.sharding_conditions[1].sharding_values[0], 2)
        self._assert_sharding_value(actual.sharding_conditions[1].sharding_values[1], 11)

    def test_optimize_without_generated_key(self):
        self.insert_statement.generate_key_column_index = 1
        actual = InsertOptimizeEngine(self.sharding_rule, self.insert_statement, self.parameters, None).optimize()
        self.assertFalse(actual.is_always_false())
        self.assertEqual(len(actual.sharding_conditions), 2)
        self.assertEqual(len(actual.sharding_conditions[0].parameters), 2)
        self.assertEqual(len(actual.sharding_conditions[1].parameters), 2)
        self.assertEqual(actual.sharding_conditions[0].parameters, [10, 'init'])
        self.assertEqual(actual.sharding_conditions[1].parameters, [11, 'init'])
        self.assertEqual(actual.sharding_conditions[0].insert_value_expression, '(?, ?)')
        self.assertEqual(actual.sharding_conditions[1].insert_value_expression, '(?, ?)')
        self.assertEqual(len(actual.sharding_conditions[0].sharding_values), 1)
        self.assertEqual(len(actual.sharding_conditions[1].sharding_values), 1)
        self._assert_sharding_value(actual.sharding_conditions[0].sharding_values[0], 10)
        self._assert_sharding_value(actual.sharding_conditions[1].sharding_values[0], 11)

    def _assert_sharding_value(self, sharding_value, value):
        self.assertEqual(sharding_value.values, [value])
