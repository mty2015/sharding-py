# -*- coding: utf-8 -*-
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
