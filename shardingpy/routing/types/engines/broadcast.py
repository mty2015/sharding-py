from shardingpy.constant import SQLType
from shardingpy.routing.types.base import RoutingResult, TableUnit, RoutingTable


class DatabaseBroadcastRoutingEngine:
    def __init__(self, sharding_rule):
        self.sharding_rule = sharding_rule

    def route(self):
        result = RoutingResult()
        for each in self.sharding_rule.sharding_data_source_names.data_source_names:
            result.table_units.table_units.append(TableUnit(each))
        return result


class TableBroadcastRoutingEngine:
    def __init__(self, sharding_rule, sql_statement):
        self.sharding_rule = sharding_rule
        self.sql_statement = sql_statement

    def route(self):
        result = RoutingResult()
        for each in self._get_logic_table_names():
            result.table_units.table_units.extend(self._get_all_table_units(each))
        return result

    def _get_logic_table_names(self):
        if self._is_operate_index_without_table():
            return [self.sharding_rule.get_logic_table_name(self._get_index_token().index_name)]

        return self.sql_statement.tables.get_table_names()

    def _is_operate_index_without_table(self):
        return self.sql_statement.sql_type == SQLType.DDL and self.sql_statement.tables.is_empty()

    def _get_index_token(self):
        assert len(self.sql_statement.sql_tokens) == 1
        return self.sql_statement.sql_tokens[0]

    def _get_all_table_units(self, logic_table_name):
        result = list()
        table_rule = self.sharding_rule.get_table_rule(logic_table_name)
        for each in table_rule.actual_data_nodes:
            table_unit = TableUnit(each.data_source_name)
            table_unit.routing_tables.append(RoutingTable(logic_table_name, each.table_name))
            result.append(table_unit)
        return result
