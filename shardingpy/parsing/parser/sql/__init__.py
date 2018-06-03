from shardingpy.parsing.parser.context.condition import Conditions
from shardingpy.parsing.parser.context.table import Tables


class SQLStatement:
    def __init__(self, sql_type):
        self.sql_type = sql_type
        self.tables = Tables()
        self.conditions = Conditions()
        self.sql_tokens = list()
        self.parameters_index = 0

    def increase_parameters_index(self):
        self.parameters_index += 1


