class ShardingMetaData(object):
    def __init__(self):
        self.table_meta_data_map = None

    def init(self, sharding_rule):
        self.table_meta_data_map = dict()
        for each in sharding_rule.table_rules:
            self.refresh(each, sharding_rule)

    def refresh(self, table_rule, sharding_rule):
        pass


class TableMetaData(object):
    def __init__(self, column_meta_data):
        self.column_meta_data = column_meta_data

    def get_all_column_names(self):
        return [each.column_name for each in self.column_meta_data]


class ColumnMetaData(object):
    def __init__(self, column_name, column_type, key_type):
        self.column_name = column_name
        self.column_type = column_type
        self.key_type = key_type


class AbstractRefreshHandler(object):
    def __init__(self, route_result, sharding_meta_data, sharding_rule):
        self.route_result = route_result
        self.sharding_meta_data = sharding_meta_data
        self.sharding_rule = sharding_rule

    def execute(self):
        raise NotImplementedError()
