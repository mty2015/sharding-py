from .dql import select, select_aggregate, select_expression, select_group_by, select_order_by, select_others
from .dml import insert

ALL_PARSER_RESULT_SET = {**select.parser_result_set, **select_aggregate.parser_result_set,
                         **select_expression.parser_result_set, **select_group_by.parser_result_set,
                         **select_order_by.parser_result_set, **select_others.parser_result_set,
                         **insert.parser_result_set}
