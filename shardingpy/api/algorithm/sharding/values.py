# -*- coding: utf-8 -*-
class ListShardingValue:
    def __init__(self, logic_table_name, column_name, values):
        self.logic_table_name = logic_table_name
        self.column_name = column_name
        self.values = values


class RangeShardingValue:
    def __init__(self, logic_table_name, column_name, value_range):
        self.logic_table_name = logic_table_name
        self.column_name = column_name
        self.value_range = value_range
