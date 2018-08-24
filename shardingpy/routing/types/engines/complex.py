from collections import defaultdict
from itertools import product
from shardingpy.exception import ShardingException
from shardingpy.routing.types.base import RoutingResult, TableUnits, TableUnit
from shardingpy.routing.types.engines.standard import StandardRoutingEngine


class ComplexRoutingEngine:
    def __init__(self, sharding_rule, logic_tables, sharding_conditions):
        self.sharding_rule = sharding_rule
        self.logic_tables = logic_tables
        self.sharding_conditions = sharding_conditions

    def route(self):
        result = list()
        binding_table_names = set()
        for each in self.logic_tables:
            table_rule = self.sharding_rule.try_find_table_rule_by_logic_table(each)
            if table_rule:
                if each not in binding_table_names:
                    result.append(StandardRoutingEngine(self.sharding_rule, each, self.sharding_conditions).route())
                binding_table_rule = self.sharding_rule.find_binding_table_rule(each)
                if binding_table_rule:
                    binding_table_names.update([e.logic_table for e in binding_table_rule.table_rules])

        if not result:
            raise ShardingException(
                "Cannot find table rule and default data source with logic tables: '{}'".format(self.logic_tables))
        if len(result) == 1:
            return result[0]

        return CartesianRoutingEngine(result).route()


class CartesianRoutingEngine:
    def __init__(self, routing_results):
        self.routing_results = routing_results

    def route(self):
        result = RoutingResult()
        for data_source, logic_tables in self._get_data_source_logic_tables_map().items():
            actual_table_groups = self._get_actual_table_groups(data_source, logic_tables)
            routing_table_groups = self._to_routing_table_groups(data_source, actual_table_groups)
            result.table_units.table_units.extend(
                self._get_table_units(data_source, product(*routing_table_groups)).table_units)
        return result

    def _get_data_source_logic_tables_map(self):
        intersection_data_sources = self._get_intersection_data_sources()
        result = defaultdict(set)
        for each in self.routing_results:
            for data_source, logic_tables in each.table_units.get_data_source_logic_tables_map(
                    intersection_data_sources).items():
                result[data_source] = result[data_source].union(logic_tables)
        return result

    def _get_intersection_data_sources(self):
        result = set()
        for each in self.routing_results:
            if not result:
                result.update(each.table_units.get_data_source_names())
            else:
                result.intersection_update(each.table_units.get_data_source_names())
        return result

    def _get_actual_table_groups(self, data_source, logic_tables):
        result = list()
        for each in self.routing_results:
            result.extend(each.table_units.get_actual_table_name_groups(data_source, logic_tables))
        return result

    def _to_routing_table_groups(self, data_source, actual_table_groups):
        return [{self._find_routing_table(data_source, t) for t in e} for e in actual_table_groups]

    def _find_routing_table(self, data_source, actual_table):
        for each in self.routing_results:
            result = each.table_units.find_routing_table(data_source, actual_table)
            if result:
                return result
        raise Exception(
            "Cannot found routing table factor, data source: {}, actual table: {}".format(data_source, actual_table))

    def _get_table_units(self, data_source, cartesian_routing_table_groups):
        result = TableUnits()
        for each in cartesian_routing_table_groups:
            table_unit = TableUnit(data_source)
            table_unit.routing_tables.extend(each)
            result.table_units.append(table_unit)
        return result
