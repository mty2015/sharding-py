from shardingpy.routing.strategy.base import NoneShardingStrategy, get_sharding_strategy
from shardingpy.keygen.base import DefaultKeyGenerator
from shardingpy.util import strutil
from shardingpy.exception import ShardingConfigurationException


class ShardingRule(object):

    def __init__(self, sharding_rule_configuration, data_source_names):
        self.sharding_rule_configuration = sharding_rule_configuration
        self.sharding_data_source_names = ShardingDatasourceNames(sharding_rule_configuration, data_source_names)
        self.table_rules = list()
        for each in sharding_rule_configuration.table_rule_configs:
            self.table_rules.append(TableRule(each, self.sharding_data_source_names))
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

    def try_find_table_rule_by_logic_table(self, logic_table_name):
        for each in self.table_rules:
            if each.logic_table == logic_table_name.lower():
                return each

    def find_binding_table_rule(self, logic_table):
        for each in self.binding_table_rules:
            if each.has_logic_table(logic_table):
                return each

    def is_sharding_column(self, column):
        if column.name in self.default_database_sharding_strategy.sharding_columns or \
                column.name in self.default_table_sharding_strategy.sharding_columns:
            return True

        for table_rule in self.table_rules:
            if not strutil.equals_ignore_case(column.table_name, table_rule.logic_table):
                continue
            # if table_rule.database_


class ShardingDatasourceNames(object):
    def __init__(self, sharding_rule_configuration, raw_data_source_names):
        self.sharding_rule_configuration = sharding_rule_configuration
        self.data_source_names = self._get_all_data_source_names(raw_data_source_names)

    def _get_all_data_source_names(self, data_source_names):
        result = list(data_source_names)
        for each in self.sharding_rule_configuration.master_slave_rule_configs:
            result.remove(each.master_data_source_name)
            for slave_name in each.slave_data_source_names:
                result.remove(slave_name)
            result.append(each.name)
        return result

    def get_default_data_source_name(self):
        return self.data_source_names[0] if len(
            self.data_source_names) == 1 else self.sharding_rule_configuration.default_data_source_name


class TableRule(object):
    def __init__(self, table_rule_configuration, sharding_datasource_names):
        self.logic_table = table_rule_configuration.logic_table.lower()
        self.table_rule_configuration = table_rule_configuration
        self.actual_data_nodes = self._generate_data_nodes(table_rule_configuration.actual_data_nodes,
                                                           sharding_datasource_names.data_source_names)

    def _generate_data_nodes(self, actual_data_nodes, data_source_names):
        result = list()
        if actual_data_nodes:
            for each in actual_data_nodes:
                result.append(DataNode(each))
        else:
            logic_table = self.logic_table
            for each in data_source_names:
                result.append(DataNode(each, logic_table))
        return result

    def get_actual_data_source_names(self):
        return [each.data_source_name for each in self.actual_data_nodes]

    def get_actual_table_names(self):
        return [each.table_name for each in self.actual_data_nodes]

    def find_actual_table_index(self, data_source_name, table_name):
        result = 0
        for each in self.actual_data_nodes:
            if strutil.equals_ignore_case(each.data_source_name, data_source_name) and strutil.equals_ignore_case(
                    each.table_name, table_name):
                return result
            result += 1
        return -1

    def is_existed(self, actual_table_name):
        return any([strutil.equals_ignore_case(actual_table_name, each.table_name) for each in self.actual_data_nodes])


class BindingTableRule(object):
    def __init__(self, table_rules):
        self.table_rules = table_rules

    def has_logic_table(self, logic_table):
        for each in self.table_rules:
            if each.logic_table == logic_table.lower():
                return True
        return False


class MasterSlaveRule(object):
    def __init__(self):
        pass


class DataNode(object):
    DELIMITER = '.'

    def __init__(self, data_node):
        if not (self.DELIMITER in data_node and len(data_node.split(self.DELIMITER)) == 2):
            raise ShardingConfigurationException('Invalid format for actual data nodes: "{}"'.format(data_node))
        self.data_source_name, self.table_name = data_node.split(self.DELIMITER)
