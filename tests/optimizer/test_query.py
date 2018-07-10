import unittest

from shardingpy.api.algorithm.sharding.values import ListShardingValue, RangeShardingValue
from shardingpy.constant import ShardingOperator
from shardingpy.optimizer.query_optimizer import QueryOptimizeEngine
from shardingpy.parsing.parser.context.condition import Condition, Column, AndCondition, OrCondition
from shardingpy.parsing.parser.expressionparser import SQLNumberExpression


class QueryOptimizeEngineTest(unittest.TestCase):

    def test_optimize_always_false_list_conditions(self):
        condition1 = Condition(Column('column', 'tbl'), ShardingOperator.IN, SQLNumberExpression(1),
                               SQLNumberExpression(2))
        condition2 = Condition(Column('column', 'tbl'), ShardingOperator.EQUAL, SQLNumberExpression(3))
        and_condition = AndCondition()
        and_condition.conditions.extend([condition1, condition2])
        or_condition = OrCondition()
        or_condition.and_conditions.append(and_condition)
        sharding_conditions = QueryOptimizeEngine(or_condition, []).optimize()
        self.assertTrue(sharding_conditions.is_always_false())

    def test_optimize_always_false_range_conditions(self):
        condition1 = Condition(Column('column', 'tbl'), ShardingOperator.BETWEEN, SQLNumberExpression(1),
                               SQLNumberExpression(2))
        condition2 = Condition(Column('column', 'tbl'), ShardingOperator.BETWEEN, SQLNumberExpression(3),
                               SQLNumberExpression(4))
        and_condition = AndCondition()
        and_condition.conditions.extend([condition1, condition2])
        or_condition = OrCondition()
        or_condition.and_conditions.append(and_condition)
        sharding_conditions = QueryOptimizeEngine(or_condition, []).optimize()
        self.assertTrue(sharding_conditions.is_always_false())

    def test_optimize_always_false_list_conditions_and_range_conditions(self):
        condition1 = Condition(Column('column', 'tbl'), ShardingOperator.IN, SQLNumberExpression(1),
                               SQLNumberExpression(2))
        condition2 = Condition(Column('column', 'tbl'), ShardingOperator.BETWEEN, SQLNumberExpression(3),
                               SQLNumberExpression(4))
        and_condition = AndCondition()
        and_condition.conditions.extend([condition1, condition2])
        or_condition = OrCondition()
        or_condition.and_conditions.append(and_condition)
        sharding_conditions = QueryOptimizeEngine(or_condition, []).optimize()
        self.assertTrue(sharding_conditions.is_always_false())

    def test_optimize_list_conditions(self):
        condition1 = Condition(Column('column', 'tbl'), ShardingOperator.IN, SQLNumberExpression(1),
                               SQLNumberExpression(2))
        condition2 = Condition(Column('column', 'tbl'), ShardingOperator.EQUAL, SQLNumberExpression(1))
        and_condition = AndCondition()
        and_condition.conditions.extend([condition1, condition2])
        or_condition = OrCondition()
        or_condition.and_conditions.append(and_condition)
        sharding_conditions = QueryOptimizeEngine(or_condition, []).optimize()
        self.assertFalse(sharding_conditions.is_always_false())
        self.assertEqual(len(sharding_conditions.sharding_conditions), 1)
        self.assertEqual(len(sharding_conditions.sharding_conditions[0].sharding_values), 1)
        sharding_value = sharding_conditions.sharding_conditions[0].sharding_values[0]
        self.assertTrue(isinstance(sharding_value, ListShardingValue))
        self.assertEqual(sharding_value.values, [1])

    def test_optimize_range_conditions(self):
        condition1 = Condition(Column('column', 'tbl'), ShardingOperator.BETWEEN, SQLNumberExpression(1),
                               SQLNumberExpression(2))
        condition2 = Condition(Column('column', 'tbl'), ShardingOperator.BETWEEN, SQLNumberExpression(1),
                               SQLNumberExpression(3))
        and_condition = AndCondition()
        and_condition.conditions.extend([condition1, condition2])
        or_condition = OrCondition()
        or_condition.and_conditions.append(and_condition)
        sharding_conditions = QueryOptimizeEngine(or_condition, []).optimize()
        self.assertFalse(sharding_conditions.is_always_false())
        self.assertEqual(len(sharding_conditions.sharding_conditions), 1)
        self.assertEqual(len(sharding_conditions.sharding_conditions[0].sharding_values), 1)
        sharding_value = sharding_conditions.sharding_conditions[0].sharding_values[0]
        self.assertTrue(isinstance(sharding_value, RangeShardingValue))
        self.assertEqual(sharding_value.value_range.lower, 1)
        self.assertEqual(sharding_value.value_range.upper, 2)

    def test_optimize_list_conditions_and_range_conditions(self):
        condition1 = Condition(Column('column', 'tbl'), ShardingOperator.IN, SQLNumberExpression(1),
                               SQLNumberExpression(2))
        condition2 = Condition(Column('column', 'tbl'), ShardingOperator.BETWEEN, SQLNumberExpression(1),
                               SQLNumberExpression(3))
        and_condition = AndCondition()
        and_condition.conditions.extend([condition1, condition2])
        or_condition = OrCondition()
        or_condition.and_conditions.append(and_condition)
        sharding_conditions = QueryOptimizeEngine(or_condition, []).optimize()
        self.assertFalse(sharding_conditions.is_always_false())
        self.assertEqual(len(sharding_conditions.sharding_conditions), 1)
        self.assertEqual(len(sharding_conditions.sharding_conditions[0].sharding_values), 1)
        sharding_value = sharding_conditions.sharding_conditions[0].sharding_values[0]
        self.assertTrue(isinstance(sharding_value, ListShardingValue))
        self.assertEqual(sharding_value.values, [1, 2])

