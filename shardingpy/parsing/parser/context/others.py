# -*- coding: utf-8 -*-
from shardingpy.util import strutil


class OrderItem:
    def __init__(self, owner, name, order_direction, null_order_type, alias, index=-1):
        self.owner = owner
        self.name = name
        self.order_direction = order_direction
        self.null_order_direction = null_order_type
        self.alias = alias
        self.index = index

    def get_column_label(self):
        return self.alias if self.alias else self.name

    def get_qualified_name(self):
        if self.name:
            return self.owner + '.' + self.name if self.owner else self.name

    def __eq__(self, other):
        if not other or not isinstance(other, OrderItem):
            return False
        return self.order_direction == other.order_direction and (
                self._column_label_equals(other) or self._qualified_name_equals(other) or self._index_equals(other))

    def _index_equals(self, other):
        return self.get_column_label() and strutil.equals_ignore_case(self.get_column_label(), other.get_column_label())

    def _qualified_name_equals(self, other):
        return self.get_qualified_name() and strutil.equals_ignore_case(self.get_qualified_name(),
                                                                        other.get_qualified_name())

    def _column_label_equals(self, other):
        return self.index != -1 and self.index == other.index

    def __hash__(self):
        return hash(self.name) + 17 * hash(self.alias) if self.alias else 0
