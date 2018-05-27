from shardingpy.parsing.lexer.token import Symbol

from shardingpy.util import sqlutil


class StarSelectItem:
    def __init__(self, owner=None):
        self.owner = owner
        self.alias = None

    @property
    def expression(self):
        return self.owner + '.' + Symbol.STAR.value if self.owner else Symbol.STAR.value

    def __eq__(self, other):
        return other and isinstance(other, StarSelectItem) and self.__dict__ == other.__dict__

    def __hash__(self):
        return 1


class CommonSelectItem:
    def __init__(self, expression, alias):
        self.expression = expression
        self.alias = alias

    def __eq__(self, other):
        return other and isinstance(other, CommonSelectItem) and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.expression)


class AggregationSelectItem:
    def __init__(self, aggregation_type, inner_expression, alias):
        self.aggregation_type = aggregation_type
        self.inner_expression = inner_expression
        self.alias = alias
        self.derived_aggregation_select_items = list()
        self.index = -1

    @property
    def expression(self):
        return sqlutil.get_exactly_value(self.aggregation_type.name + self.inner_expression)

    @property
    def column_label(self):
        return self.alias if self.alias else self.expression

    def __eq__(self, other):
        return other and isinstance(other, AggregationSelectItem) and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.aggregation_type) + hash(self.inner_expression)
