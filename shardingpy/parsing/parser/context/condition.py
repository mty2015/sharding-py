# -*- coding: utf-8 -*-
from collections import OrderedDict

from shardingpy.api.algorithm.sharding.values import ListShardingValue, RangeShardingValue
from shardingpy.constant import ShardingOperator
from shardingpy.exception import UnsupportedOperationException
from shardingpy.parsing.parser.expressionparser import SQLPlaceholderExpression, SQLTextExpression, SQLNumberExpression
from shardingpy.util.extype import RangeType, Range


class Column:
    def __init__(self, name, table_name):
        self.name = name
        self.table_name = table_name

    def __eq__(self, other):
        return other and isinstance(other, Column) and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.name)


class Condition:
    def __init__(self, column, operator, *sql_expressions):
        assert isinstance(column, Column)
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

    def get_sharding_value(self, parameters):
        condition_values = self._get_values(parameters)
        if self.operator in [ShardingOperator.EQUAL, ShardingOperator.IN]:
            return ListShardingValue(self.column.table_name, self.column.name, condition_values)
        elif self.operator == ShardingOperator.BETWEEN:
            return RangeShardingValue(self.column.table_name, self.column.name,
                                      Range(condition_values[0], RangeType.CLOSED, condition_values[1],
                                            RangeType))
        else:
            raise UnsupportedOperationException("sharding condition not support :" + self.operator.value)

    def _get_values(self, parameters):
        result = self._values[:]
        for position, param_index in self._position_index_map.items():
            parameter = parameters[param_index]
            if position < len(result):
                result.insert(position, parameter)
            else:
                result.append(parameter)
        return result


class Conditions:
    def __init__(self, conditions=None):
        self.conditions = dict()
        if conditions:
            self.conditions = conditions.copy()

    def add(self, condition, sharding_rule):
        if sharding_rule.is_sharding_column(condition.column):
            self.conditions[condition.column] = condition