# -*- coding: utf-8 -*-
from shardingpy.util import sqlutil


class TableToken:
    def __init__(self, begin_position, original_literals):
        self.begin_position = begin_position
        self._original_literals = original_literals

    @property
    def table_name(self):
        return sqlutil.get_exactly_value(self._original_literals)
