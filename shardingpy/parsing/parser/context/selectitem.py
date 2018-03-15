from shardingpy.parsing.token import Symbol

from shardingpy.util import sqlutil


class StarSelectItem:
    def __init__(self, owner=None):
        self.owner = owner
        self.alias = None

    @property
    def expression(self):
        return self.owner + '.' + Symbol.STAR.value if self.owner else Symbol.STAR.value


class CommonSelectItem:
    def __init__(self, expression, alias):
        self.expression = expression
        self.alias = alias


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
