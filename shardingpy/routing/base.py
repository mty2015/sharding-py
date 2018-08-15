from shardingpy.constant import SQLType


class SQLUnit:
    def __init__(self, sql, parameter_sets):
        self.sql = sql
        self.parameter_sets = parameter_sets  # two dimensional data


class SQLExecutionUnit:
    def __init__(self, data_source, sql_unit):
        self.data_source = data_source
        self.sql_unit = sql_unit


class SQLRouteResult:
    def __init__(self, sql_statement, generated_key):
        self.sql_statement = sql_statement
        self.generated_key = generated_key
        self.execution_units = list()

    def can_refresh_meta_data(self):
        return self.sql_statement.sql_type == SQLType.DDL and not self.sql_statement.tables.is_empty()
