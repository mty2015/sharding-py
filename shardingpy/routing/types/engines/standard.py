from itertools import chain
from shardingpy.optimizer.insert_optimizer import InsertShardingCondition
from shardingpy.routing.types.base import RoutingResult, TableUnit, RoutingTable
from shardingpy.rule.base import DataNode
from shardingpy.util.types import OrderedSet


class StandardRoutingEngine:
    def __init__(self, sharding_rule, logic_table_name, sharding_conditions):
        self.sharding_rule = sharding_rule
        self.logic_table_name = logic_table_name
        self.sharding_conditions = sharding_conditions

    def route(self):
        table_rule = self.sharding_rule.get_table_rule(self.logic_table_name)
        database_sharding_columns = self.sharding_rule.get_database_sharding_strategy(table_rule).get_sharding_columns()
        table_sharding_columns = self.sharding_rule.get_table_sharding_strategy(table_rule).get_sharding_columns()
        routed_data_nodes = list()
        if not self.sharding_conditions.sharding_conditions:
            routed_data_nodes.extend(self._route(table_rule, list(), list()))
        else:
            for each in self.sharding_conditions.sharding_conditions:
                database_sharding_values = self._get_sharding_values(database_sharding_columns, each)
                table_sharding_values = self._get_sharding_values(table_sharding_columns, each)
                data_nodes = self._route(table_rule, database_sharding_values, table_sharding_values)
                routed_data_nodes.extend(data_nodes)
                if isinstance(each, InsertShardingCondition):
                    each.data_nodes.extend(data_nodes)
        return self._generate_routing_result(list(OrderedSet(routed_data_nodes)))

    def _route(self, table_rule, database_sharding_values, table_sharding_values):
        routed_data_sources = self._route_data_sources(table_rule, database_sharding_values)
        return list(chain(*[self._route_tables(table_rule, e, table_sharding_values) for e in routed_data_sources]))

    def _route_data_sources(self, table_rule, database_sharding_values):
        available_target_databases = table_rule.get_actual_data_source_names()
        if not available_target_databases:
            return available_target_databases
        result = list(OrderedSet(
            self.sharding_rule.get_database_sharding_strategy(table_rule).do_sharding(available_target_databases,
                                                                                      database_sharding_values)))
        assert result, 'no database route info'
        return result

    def _route_tables(self, table_rule, routed_data_source, table_sharding_values):
        available_target_tables = table_rule.get_actual_table_names(routed_data_source)
        if not table_sharding_values:
            routed_tables = available_target_tables
        else:
            routed_tables = list(OrderedSet(
                self.sharding_rule.get_table_sharding_strategy.do_sharding(available_target_tables,
                                                                           table_sharding_values)))
        return [DataNode(routed_data_source, e) for e in routed_tables]

    def _get_sharding_values(self, sharding_columns, sharding_condition):
        return [e for e in sharding_condition.sharding_values if
                self.logic_table_name == e.logic_table_name and e.column_name in sharding_columns]

    def _generate_routing_result(self, routed_data_nodes):
        result = RoutingResult()
        for each in routed_data_nodes:
            table_unit = TableUnit(each.data_source_name)
            table_unit.routing_tables.append(RoutingTable(self.logic_table_name, each.table_name))
            result.table_units.table_units.append(table_unit)
        return result
