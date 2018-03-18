# -*- coding: utf-8 -*-
from shardingpy.util import sqlutil


class TableToken:
    def __init__(self, begin_position, original_literals):
        self.begin_position = begin_position
        self._original_literals = original_literals

    @property
    def table_name(self):
        return sqlutil.get_exactly_value(self._original_literals)


class OffsetToken:
    def __init__(self, begin_position, offset):
        self.begin_position = begin_position
        self.offset = offset


class RowCountToken:
    def __init__(self, begin_position, row_count):
        self.begin_position = begin_position
        self.row_count = row_count


class ItemsToken:
    def __init__(self, begin_position):
        self.begin_position = begin_position
        self.items = list()


class OrderByToken:
    def __init__(self, begin_position):
        self.begin_position = begin_position
