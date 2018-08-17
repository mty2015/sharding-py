from shardingpy.constant import SQLType


class SQLUnit:
    def __init__(self, sql, parameter_sets):
        self.sql = sql
        self.parameter_sets = parameter_sets  # two dimensional data

    def __eq__(self, other):
        if not isinstance(other, SQLUnit):
            return False
        return self.sql == other.sql and self.parameter_sets == self.parameter_sets

    def __hash__(self):
        return hash(self.sql) + 17 * hash(self.parameter_sets) if self.parameter_sets else 0


class SQLExecutionUnit:
    def __init__(self, data_source, sql_unit):
        self.data_source = data_source
        self.sql_unit = sql_unit

    def __eq__(self, other):
        if not isinstance(other, SQLExecutionUnit):
            return False
        return self.data_source == other.data_source and self.sql_unit == other.sql_unit

    def __hash__(self):
        return hash(self.data_source) + 17 * hash(self.sql_unit)


class SQLRouteResult:
    def __init__(self, sql_statement, generated_key):
        self.sql_statement = sql_statement
        self.generated_key = generated_key
        self.execution_units = list()

    def can_refresh_meta_data(self):
        return self.sql_statement.sql_type == SQLType.DDL and not self.sql_statement.tables.is_empty()
