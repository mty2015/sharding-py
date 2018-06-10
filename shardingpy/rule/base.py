from shardingpy.routing.strategy.base import NoneShardingStrategy, get_sharding_strategy
from shardingpy.keygen.base import DefaultKeyGenerator


class ShareRule(object):

    def __init__(self, sharding_rule_configuration, datasource_names):
        self.sharding_rule_configuration = sharding_rule_configuration
        self.sharding_datasource_names = ShardingDatasourceNames(sharding_rule_configuration, datasource_names)
        self.table_rules = list()
        for each in sharding_rule_configuration.table_rule_configs:
            self.table_rules.append(TableRule(each, self.sharding_datasource_names))
        self.binding_table_rules = list()
        for group in sharding_rule_configuration.binding_tables_groups:
            self.binding_table_rules.append(BindingTableRule(
                [self.get_table_rule(logic_table_name_for_binding) for logic_table_name_for_binding in group]))
        self.default_database_sharding_strategy = get_sharding_strategy(
            sharding_rule_configuration.default_database_sharding_strategy_config) \
            if sharding_rule_configuration.default_database_sharding_strategy_config else NoneShardingStrategy
        self.default_table_sharding_strategy = get_sharding_strategy(
            sharding_rule_configuration.default_table_sharding_strategy_config) \
            if sharding_rule_configuration.default_table_sharding_strategy_config else NoneShardingStrategy
        self.default_key_generator = sharding_rule_configuration.default_key_generator \
            if sharding_rule_configuration.default_key_generator else DefaultKeyGenerator()
        self.master_slave_rules = list()
        for each in sharding_rule_configuration.master_slave_rule_configs:
            self.master_slave_rules.append(MasterSlaveRule(each))

    def get_table_rule(self, logic_table_name):
        pass


class ShardingDatasourceNames(object):
    def __init__(self, sharding_rule_configuration, datasource_names):
        self.sharding_rule_configuration = sharding_rule_configuration
        self.datasource_name = datasource_names


class TableRule(object):
    def __init__(self, sharding_rule_configuration, sharding_datasource_names):
        self.sharding_rule_configuration = sharding_rule_configuration
        self.sharding_datasource_names = sharding_datasource_names


class BindingTableRule(object):
    def __init__(self, table_rules):
        self.table_rules = table_rules


class MasterSlaveRule(object):
    def __init__(self):
        pass
