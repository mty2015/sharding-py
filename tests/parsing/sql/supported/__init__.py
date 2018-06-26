from . import dml
from .dql import select, select_aggregate, select_expression

SQL_CASES = {**select.CASES, **select_aggregate.CASES, **select_expression.CASES, **dml.CASES}
