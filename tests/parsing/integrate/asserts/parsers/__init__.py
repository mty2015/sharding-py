from .dql import select, select_aggregate, select_expression

ALL_PARSER_RESULT_SET = {**select.parser_result_set, **select_aggregate.parser_result_set,
                         **select_expression.parser_result_set}
