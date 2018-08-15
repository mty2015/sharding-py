class GeneratedKey:
    def __init__(self, column):
        self.column = column
        self.generated_keys = list()


class ShardingRouter:

    def parse(self, logic_sql, user_cache):
        raise NotImplementedError

    def route(self, logic_sql, parameters, sql_statement):
        raise NotImplementedError

