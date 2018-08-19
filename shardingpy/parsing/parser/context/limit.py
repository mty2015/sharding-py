# -*- coding: utf-8 -*-
from shardingpy.constant import DatabaseType
from shardingpy.exception import SQLParsingException


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
        return self.offset.value if self.offset else 0

    def is_need_rewrite_row_count(self):
        return self.database_type in [DatabaseType.MySQL, DatabaseType.H2]

    def process_parameters(self, parameters, is_fetch_all):
        self._fill(parameters)
        self.rewrite(parameters, is_fetch_all)

    def _fill(self, parameters):
        _offset = 0
        if self.offset:
            _offset = self._get_offset_value() if self.offset.index == -1 else parameters[self.offset.index]
            self.offset.value = _offset
        _row_count = 0
        if self.row_count:
            _row_count = self._get_row_count_value() if self.row_count.index == -1 else parameters[self.row_count.index]
            self.row_count.value = _row_count
        if _offset < 0 or _row_count < 0:
            raise SQLParsingException('LIMIT offset and row count can not be a negative value.')

    def rewrite(self, parameters, is_fetch_all):
        rewrite_offset = 0
        if is_fetch_all:
            rewrite_row_count = 0x7fffffff
        elif self._is_need_rewrite_row_count():
            rewrite_row_count = self.get_offset_value() + self.row_count.value if self.row_count else -1
        else:
            rewrite_row_count = self.row_count.value

        if self.offset and self.offset.index > -1:
            parameters.insert(self.offset.index, rewrite_offset)
        if self.row_count and self.row_count.index > -1:
            parameters.insert(self.row_count.index, rewrite_row_count)

    def _get_offset_value(self):
        return self.offset.value if self.offset else 0

    def _get_row_count_value(self):
        return self.row_count.value if self.row_count else -1
