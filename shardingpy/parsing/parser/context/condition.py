# -*- coding: utf-8 -*-
from collections import OrderedDict, defaultdict

from shardingpy.api.algorithm.sharding.values import ListShardingValue, RangeShardingValue
from shardingpy.constant import ShardingOperator
from shardingpy.exception import UnsupportedOperationException
from shardingpy.parsing.parser.expressionparser import SQLPlaceholderExpression, SQLTextExpression, SQLNumberExpression
from shardingpy.util.extype import RangeType, Range
from shardingpy.util.strutil import equals_ignore_case


class Column:
    def __init__(self, name, table_name):
        self.name = name
        self.table_name = table_name

    def __eq__(self, other):
        return other and isinstance(other, Column) and equals_ignore_case(self.name, other.name) and equals_ignore_case(
            self.table_name, other.table_name)

    def __hash__(self):
        return hash(self.name) + 17 * hash(self.table_name) if self.table_name else 0


class Condition:
    def __init__(self, column, operator, *sql_expressions):
        if column:
            assert isinstance(column, Column)
        if operator:
            assert isinstance(operator, ShardingOperator)
        self.column = column
        self.operator = operator
        self._position_index_map = OrderedDict()
        self._values = list()
        position = 0
        for expr in sql_expressions:
            if isinstance(expr, SQLPlaceholderExpression):
                self._position_index_map[position] = expr.index
            elif isinstance(expr, SQLTextExpression):
                self._values.append(expr.text)
            elif isinstance(expr, SQLNumberExpression):
                self._values.append(expr.number)
            position += 1

    # Deprecated
    def get_sharding_value(self, parameters):
        condition_values = self.get_condition_values(parameters)
        if self.operator in [ShardingOperator.EQUAL, ShardingOperator.IN]:
            return ListShardingValue(self.column.table_name, self.column.name, condition_values)
        elif self.operator == ShardingOperator.BETWEEN:
            return RangeShardingValue(self.column.table_name, self.column.name,
                                      Range(condition_values[0], RangeType.CLOSED, condition_values[1],
                                            RangeType))
        else:
            raise UnsupportedOperationException("sharding condition not support :" + self.operator.value)

    def get_condition_values(self, parameters):
        result = self._values[:]
        for position, param_index in self._position_index_map.items():
            parameter = parameters[param_index]
            if position < len(result):
                result.insert(position, parameter)
            else:
                result.append(parameter)
        return result


class AndCondition(object):
    def __init__(self):
        self.conditions = list()

    def get_conditions_map(self):
        result = defaultdict(list)
        for each in self.conditions:
            result[each.column].append(each)
        return result

    def optimize(self):
        result = AndCondition()
        result.conditions = [each for each in self.conditions if type(each) == Condition]
        if not result.conditions:
            result.conditions.append(NullCondition())
        return result


class OrCondition(object):
    def __init__(self, condition=None):
        self.and_conditions = list()
        if condition:
            self.add(condition)

    def add(self, condition):
        assert isinstance(condition, Condition)
        if len(self.and_conditions) == 0:
            self.and_conditions.append(AndCondition())
        self.and_conditions[0].conditions.append(condition)

    def find(self, column, index):
        pass


class Conditions:
    def __init__(self, conditions=None):
        self.or_condition = OrCondition()
        if conditions:
            self.or_condition.and_conditions.extend(conditions.or_condition.and_conditions)

    def add(self, condition, sharding_rule):
        if sharding_rule.is_sharding_column(condition.column):
            self.or_condition.add(condition)


class NullCondition(Condition):
    def __init__(self):
        super().__init__(None, None)


class GeneratedKeyCondition(Condition):
    def __init__(self, column, index, value):
        super().__init__(column, ShardingOperator.EQUAL, SQLNumberExpression(value))
        self.index = index
        self.value = value
