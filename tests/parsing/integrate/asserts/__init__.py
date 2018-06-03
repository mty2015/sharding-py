from .parsers import ALL_PARSER_RESULT_SET


def get_parser_result(sql_case_id):
    assert sql_case_id in ALL_PARSER_RESULT_SET, "sql_case_id not exist: {}".format(sql_case_id)
    return ALL_PARSER_RESULT_SET[sql_case_id]
