from shardingpy.exception import ShardingException
from shardingpy.rewrite.placeholder import ShardingPlaceholder, TablePlaceholder, SchemaPlaceholder, IndexPlaceholder, \
    InsertValuesPlaceholder
from shardingpy.routing.base import SQLUnit


class StringBuffer:
    def __init__(self):
        self.s = ''

    def __add__(self, other):
        self.s += str(other)
        return self

    def __repr__(self):
        return self.s


class SQLBuilder:
    def __init__(self, parameters=None):
        self.parameters = parameters or list()
        self.segments = list()
        self.current_segment = StringBuffer()
        self.segments.append(self.current_segment)

    def append_literals(self, literals):
        self.current_segment += literals
        return self

    def append_placeholder(self, sharding_placeholder):
        self.segments.append(sharding_placeholder)
        self.current_segment = StringBuffer()
        self.segments.append(self.current_segment)
        return self

    def to_sql(self, table_unit, logic_and_actual_table_map, sharding_rule):
        insert_parameters = list()
        result = ""
        for each in self.segments:
            if not isinstance(each, ShardingPlaceholder):
                result += str(each)
                continue

            logic_table_name = each.logic_table_name
            actual_table_name = logic_and_actual_table_map.get(logic_table_name)
            if isinstance(each, TablePlaceholder):
                result += (actual_table_name if actual_table_name else logic_table_name)
            elif isinstance(each, SchemaPlaceholder):
                table_rule = sharding_rule.try_find_table_rule_by_actual_table(actual_table_name)
                if not table_rule and not sharding_rule.sharding_data_source_names.get_default_data_source_name():
                    raise ShardingException("Cannot found schema name '%s' in sharding rule." % each.logic_table_name)

                result += table_rule.get_actual_data_source_names()[0]
            elif isinstance(each, IndexPlaceholder):
                result += each.logic_index_name
                if actual_table_name:
                    result += ('_' + actual_table_name)
            elif isinstance(each, InsertValuesPlaceholder):
                expressions = list()
                for sharding_condition in each.sharding_conditions.sharding_conditions:
                    self._process_insert_sharding_condition(table_unit, sharding_condition, expressions,
                                                            insert_parameters)
                count = 0
                for s in expressions:
                    if count != 0:
                        result += ', '
                    result += s
                    count += 1
            else:
                result += str(each)
        if not insert_parameters:
            return SQLUnit(result, [self.parameters])
        else:
            return SQLUnit(result, [insert_parameters])

    def _process_insert_sharding_condition(self, table_unit, insert_sharding_condition, expressions, insert_parameters):
        for data_node in insert_sharding_condition.data_nodes:
            if data_node.data_source_name == table_unit.data_source_name and data_node.table_name == \
                    table_unit.routing_tables[0].actual_table_name:
                expressions.append(insert_sharding_condition.insert_value_expression)
                insert_parameters.extend(insert_sharding_condition.parameters)
                break
