from shardingpy.routing.types.base import RoutingResult, TableUnit, RoutingTable


class UnicastRoutingEngine:
    def __init__(self, sharding_rule, logic_tables):
        self.sharding_rule = sharding_rule
        self.logic_tables = logic_tables

    def route(self):
        result = RoutingResult()
        if not self.logic_tables:
            result.table_units.table_units.append(
                TableUnit(self.sharding_rule.sharding_data_source_names.data_source_names[0]))
        elif len(self.logic_tables) == 1:
            logic_table_name = self.logic_tables[0]
            data_node = self.sharding_rule.find_data_node_by_logic_table_name(logic_table_name)
            table_unit = TableUnit(data_node.data_source_name)
            table_unit.routing_tables.append(RoutingTable(logic_table_name, data_node.table_name))
            result.table_units.table_units.append(table_unit)
        else:
            routing_tables = list()
            data_source_name = None
            for each in self.logic_tables:
                data_node = self.sharding_rule.find_data_node(data_source_name, each)
                routing_tables.append(RoutingTable(each, data_node.table_name))
                if not data_source_name:
                    data_source_name = data_node.data_source_name
                table_unit = TableUnit(data_source_name)
                table_unit.routing_tables.extend(routing_tables)
                result.table_units.table_units.append(table_unit)
        return result
