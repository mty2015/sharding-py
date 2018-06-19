# -*- coding: utf-8 -*-
from shardingpy.util import sqlutil


class SQLToken:
    def __init__(self, begin_position):
        self.begin_position = begin_position


class TableToken(SQLToken):
    def __init__(self, begin_position, skipped_schema_name_length, original_literals):
        super().__init__(begin_position)
        self.skipped_schema_name_length = skipped_schema_name_length
        self.original_literals = original_literals

    @property
    def table_name(self):
        return sqlutil.get_exactly_value(self.original_literals)


class OffsetToken(SQLToken):
    def __init__(self, begin_position, offset):
        super().__init__(begin_position)
        self.offset = offset


class RowCountToken(SQLToken):
    def __init__(self, begin_position, row_count):
        super().__init__(begin_position)
        self.row_count = row_count


class ItemsToken(SQLToken):
    def __init__(self, begin_position):
        super().__init__(begin_position)
        self.is_first_of_items_special = False
        self.items = list()


class OrderByToken(SQLToken):
    pass


class IndexToken(SQLToken):
    def __init__(self, begin_position, table_name, original_literals):
        super().__init__(begin_position)
        self._table_name = table_name
        self.original_literals = original_literals

    @property
    def table_name(self):
        return sqlutil.get_exactly_value(self._table_name)

    @property
    def index_name(self):
        return sqlutil.get_exactly_value(self.original_literals)


class GeneratedKeyToken(SQLToken):
    pass


class InsertValuesToken(SQLToken):
    def __init__(self, begin_position, table_name):
        super().__init__(begin_position)
        self._table_name = table_name
