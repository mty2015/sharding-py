# -*- coding: utf-8 -*-
from shardingpy.constant import DatabaseType


class LimitValue:
    def __init__(self, value, index, bound_opened):
        self.value = value
        self.index = index
        self.bound_opened = bound_opened


class Limit:
    def __init__(self, database_type, offset, row_count):
        self.database_type = database_type
        self.offset = offset
        self.row_count = row_count

    def get_offset_value(self):
        return self.offset.vaue if self.offset else 0

    def is_need_rewrite_row_count(self):
        return self.database_type in [DatabaseType.MySQL, DatabaseType.H2]
