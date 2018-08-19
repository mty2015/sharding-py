from shardingpy.util import strutil


class RoutingTable:
    def __init__(self, logic_table_name, actual_table_name):
        self.logic_table_name = logic_table_name
        self.actual_table_name = actual_table_name


class TableUnit:
    def __init__(self, data_source_name):
        self.data_source_name = data_source_name
        self.routing_tables = list()  # RoutingTable

    def find_routing_table(self, data_source_name, actual_table_name):
        for each in self.routing_tables:
            if strutil.equals_ignore_case(self.data_source_name, data_source_name) and strutil.equals_ignore_case(
                    each.actual_table_name, actual_table_name):
                return each

    def get_actual_table_names(self, data_source_name, logic_table_name):
        if not strutil.equals_ignore_case(self.data_source_name, data_source_name):
            return set()
        return set([each.actual_table_name for each in self.routing_tables if
                    strutil.equals_ignore_case(each.logic_table_name, logic_table_name)])

    def get_logic_table_names(self, data_source_name):
        return [each.logic_table_name for each in self.routing_tables if
                strutil.equals_ignore_case(self.data_source_name, data_source_name)]


class TableUnits:
    def __init__(self):
        self.table_units = list()


class RoutingResult:
    def __init__(self):
        self.table_units = TableUnits()

    def is_single_routing(self):
        return len(self.table_units.table_units) == 1
