import enum

from tests.parsing.sql.supported import SQL_CASES as SUPPORTED_SQL_CASES
from tests.parsing.sql.unsupported import SQL_CASES as UNSUPPORTED_SQL_CASES


class SQLCaseType(enum.IntEnum):
    Literal = 0
    Placeholder = 1


def get_supported_sql_test_parameters(all_database_types):
    return _get_test_parameters(SUPPORTED_SQL_CASES, all_database_types)


def _get_test_parameters(sql_cases, all_database_types):
    """
    生成组合测试案例: [(sql_case_id, DatabaseType, SQLCaseType), ...]

    :param all_database_types: 当sql_case中没有指定database_type时，默认的支持数据库类型
    :return: [(case_id, DatabaseType, SQLCaseType), ...]
    """
    for sql_case_id, value in sql_cases.items():
        sql, database_type = value
        if not database_type:
            database_type = all_database_types
        for database_type in database_type:
            for sql_case_type in SQLCaseType:
                yield sql_case_id, database_type, sql_case_type


def get_supported_sql(sql_case_id, sql_case_type, parameters):
    return _get_sql(SUPPORTED_SQL_CASES, sql_case_id, sql_case_type, parameters)


def get_unsupported_sql(sql_case_id, sql_case_type, parameters):
    return _get_sql(UNSUPPORTED_SQL_CASES, sql_case_id, sql_case_type, parameters)


def _get_sql(sql_case_map, sql_case_id, sql_case_type, parameters):
    sql = sql_case_map[sql_case_id][0]
    if sql_case_type == SQLCaseType.Literal:
        return (sql % parameters).replace('%%', '%') if parameters else sql
    else:
        return sql.replace('%s', '?').replace('%%', '%')

