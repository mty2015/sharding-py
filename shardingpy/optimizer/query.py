from shardingpy.api.algorithm.sharding.values import RangeShardingValue, ListShardingValue
from shardingpy.constant import ShardingOperator
from shardingpy.optimizer.condition import ShardingConditions, ShardingCondition, AlwaysFalseShardingCondition
from shardingpy.parsing.parser.context.condition import OrCondition
from shardingpy.util.extype import Range, RangeType


class AlwaysFalseShardingValue:
    def __init__(self):
        self.logic_table_name = ''
        self.column_name = ''


class QueryOptimizeEngine:
    def __init__(self, or_condition, parameters):
        assert isinstance(or_condition, OrCondition)
        self.or_condition = or_condition
        self.parameters = parameters

    def optimize(self):
        sharding_conditions = [self._optimize_condition(i.get_conditions_map()) for i in
                               self.or_condition.and_conditions]
        return ShardingConditions(sharding_conditions)

    def _optimize_condition(self, conditions_map):
        result = ShardingCondition()
        for column, conditions in conditions_map.items():
            sharding_value = self._optimize_sharding_value(column, conditions)
            if isinstance(sharding_value, AlwaysFalseShardingValue):
                return AlwaysFalseShardingCondition()
            result.sharding_values.append(sharding_value)
        return result

    def _optimize_sharding_value(self, column, conditions):
        list_value = range_value = None
        for each in conditions:
            condition_values = each.get_condition_values(self.parameters)
            if each.operator in [ShardingOperator.EQUAL, ShardingOperator.IN]:
                list_value = self._optimize_list_value(condition_values, list_value)
                if not list_value:
                    return AlwaysFalseShardingValue()
            if each.operator == ShardingOperator.BETWEEN:
                range_value = self._optimize_range_value(
                    Range(condition_values[0], RangeType.CLOSED, condition_values[1], RangeType.CLOSED), range_value)
                if not range_value:
                    return AlwaysFalseShardingValue()

        if not list_value:
            return RangeShardingValue(column.table_name, column.name, range_value)
        if not range_value:
            return ListShardingValue(column.table_name, column.name, list_value)
        list_value = self._optimize_list_and_range_value(list_value, range_value)
        return ListShardingValue(column.table_name, column.name,
                                 list_value) if list_value else AlwaysFalseShardingValue()

    def _optimize_list_value(self, list_value1, list_value2):
        if not list_value2:
            return list_value1
        return [i for i in list_value1 if i in list_value2]

    def _optimize_range_value(self, range_value1, range_value2):
        return range_value1 if not range_value2 else range_value1.intersection(range_value2)

    def _optimize_list_and_range_value(self, list_value, range_value):
        return [i for i in list_value if range_value.contains(i)]
