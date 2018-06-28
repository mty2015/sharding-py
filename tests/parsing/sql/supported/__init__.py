from . import dml
from .dql import select, select_aggregate, select_expression, select_group_by, select_order_by, select_others

SQL_CASES = {**select.CASES, **select_aggregate.CASES, **select_expression.CASES, **select_group_by.CASES,
             **select_order_by.CASES, **select_others.CASES, **dml.CASES}
