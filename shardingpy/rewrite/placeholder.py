class ShardingPlaceholder:
    def __init__(self, logic_table_name):
        self.logic_table_name = logic_table_name


class TablePlaceholder(ShardingPlaceholder):
    def __repr__(self):
        return self.logic_table_name


class SchemaPlaceholder(ShardingPlaceholder):
    def __init__(self, logic_schema_name, logic_table_name):
        self.logic_schema_name = logic_schema_name
        super().__init__(logic_table_name)


class IndexPlaceholder(ShardingPlaceholder):
    def __init__(self, logic_index_name, logic_table_name):
        self.logic_index_name = logic_index_name
        super().__init__(logic_table_name)


class InsertValuesPlaceholder(ShardingPlaceholder):
    def __init__(self, logic_table_name, sharding_conditions):
        self.sharding_conditions = sharding_conditions
        super().__init__(logic_table_name)
