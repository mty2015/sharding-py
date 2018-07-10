from shardingpy.api.algorithm.sharding.values import ListShardingValue
from shardingpy.optimizer.condition import ShardingCondition, ShardingConditions
from shardingpy.parsing.parser.context.condition import GeneratedKeyCondition


class InsertShardingCondition(ShardingCondition):
    def __init__(self, insert_value_expression, parameters):
        super().__init__()
        self.insert_value_expression = insert_value_expression
        self.parameters = parameters
        self.data_nodes = list()


class InsertOptimizeEngine:
    def __init__(self, sharding_rule, insert_statement, parameters, generated_key):
        self.sharding_rule = sharding_rule
        self.insert_statement = insert_statement
        self.parameters = parameters
        self.generated_key = generated_key

    def optimize(self):
        and_conditions = self.insert_statement.conditions.or_condition.and_conditions
        insert_values = self.insert_statement.insert_values.insert_values
        result = list()
        generated_keys = None
        count = 0
        for each in and_conditions:
            insert_value = insert_values[count]
            current_parameters = self.parameters[
                                 count * insert_value.parameters_count: (count + 1) * insert_value.parameters_count]
            logic_table_name = self.insert_statement.tables.get_single_table_name()
            generate_key_column = self.sharding_rule.get_generate_key_column(logic_table_name)
            if self.insert_statement.generate_key_column_index != -1 or not generate_key_column:
                insert_sharding_condition = InsertShardingCondition(insert_value.expression, current_parameters)
            else:
                if not generated_keys:
                    generated_keys = iter(self.generated_key.generated_keys)
                current_generated_key = next(generated_keys)
                if not self.parameters:
                    expression = insert_value.expression[:len(insert_value.expression) - 1] + ', ' + str(
                        current_generated_key) + ')'
                else:
                    expression = insert_value.expression[:len(insert_value.expression) - 1] + ', ?)'
                    current_parameters.append(current_generated_key)
                insert_sharding_condition = InsertShardingCondition(expression, current_parameters)
                insert_sharding_condition.sharding_values.append(
                    self._get_sharding_condition_by_column(generate_key_column, current_generated_key))

            insert_sharding_condition.sharding_values.extend(self._get_sharding_condition_by_and_condition(each))
            result.append(insert_sharding_condition)
            count += 1

        return ShardingConditions(result)

    def _get_sharding_condition_by_column(self, column, value):
        return ListShardingValue(column.table_name, column.name,
                                 GeneratedKeyCondition(column, -1, value).get_condition_values(self.parameters))

    def _get_sharding_condition_by_and_condition(self, and_condition):
        return [ListShardingValue(i.column.table_name, i.column.name, i.get_condition_values(self.parameters)) for i in
                and_condition.conditions]
