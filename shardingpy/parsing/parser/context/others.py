# -*- coding: utf-8 -*-
class OrderItem:
    def __init__(self, owner, name, order_type, null_order_type, alias, index=-1):
        self.owner = owner
        self.name = name
        self.order_type = order_type
        self.null_order_type = null_order_type
        self.alias = alias
        self.index = index

    def get_column_label(self):
        return self.alias if self.alias else self.name

    def get_qualified_name(self):
        if self.name:
            return self.owner + '.' + self.name if self.owner else self.name

    def __eq__(self, other):
        return other and isinstance(other, OrderItem) and self.__dict__ == other.__dict__
