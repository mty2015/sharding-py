from functools import reduce

from shardingpy.util import strutil


class RoutingTable:
    def __init__(self, logic_table_name, actual_table_name):
        self.logic_table_name = logic_table_name
        self.actual_table_name = actual_table_name

    def __eq__(self, other):
        if not isinstance(other, RoutingTable):
            return False
        return self.logic_table_name == other.logic_table_name and self.actual_table_name == other.actual_table_name

    def __hash__(self):
        return hash(self.logic_table_name) + 17 * hash(self.actual_table_name)


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
        return {each.actual_table_name for each in self.routing_tables if
                strutil.equals_ignore_case(each.logic_table_name, logic_table_name)}

    def get_logic_table_names(self, data_source_name):
        return {each.logic_table_name for each in self.routing_tables if
                strutil.equals_ignore_case(self.data_source_name, data_source_name)}


class TableUnits:
    def __init__(self):
        self.table_units = list()

    def get_data_source_names(self):
        return {e.data_source_name for e in self.table_units}

    def get_data_source_logic_tables_map(self, data_source_names):
        result = {e: self._get_logic_table_names(e) for e in data_source_names}
        return {k: v for k, v in result.items() if v}

    def _get_logic_table_names(self, data_source_name):
        return reduce(lambda a, b: a.union(b), [e.get_logic_table_names(data_source_name) for e in self.table_units if
                                         strutil.equals_ignore_case(data_source_name, e.data_source_name)])

    def get_actual_table_name_groups(self, data_source_name, logic_table_names):
        result = list()
        for each in logic_table_names:
            actual_table_names = self._get_actual_table_names(data_source_name, each)
            if actual_table_names:
                result.append(actual_table_names)
        return result

    def _get_actual_table_names(self, data_source_name, logic_table_name):
        result = set()
        for each in self.table_units:
            result.update(each.get_actual_table_names(data_source_name, logic_table_name))
        return result

    def find_routing_table(self, data_source_name, actual_table_name):
        for each in self.table_units:
            result = each.find_routing_table(data_source_name, actual_table_name)
            if result:
                return result



class RoutingResult:
    def __init__(self):
        self.table_units = TableUnits()

    def is_single_routing(self):
        return len(self.table_units.table_units) == 1
