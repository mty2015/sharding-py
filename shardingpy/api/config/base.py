class ShardingRuleConfiguration(object):
    def __init__(self):
        self.default_data_source_name = None
        self.table_rule_configs = list()
        self.binding_tables_groups = list()
        self.default_database_sharding_strategy_config = None
        self.default_table_sharding_strategy_config = None
        self.master_slave_rule_configs = list()
        self.default_key_generator = None
        self.config_map = dict()
        self.props = dict()


class TableRuleConfiguration(object):
    def __init__(self):
        self.logic_table = None
        self.actual_data_nodes = None
        self.database_strategy_config = None
        self.table_strategy_config = None
        self.key_generator_name = None
        self.key_generator = None
        self.logic_index = None


def load_sharding_rule_config_from_dict(cfg):
    config = ShardingRuleConfiguration()

    config.default_data_source_name = cfg.get('default_data_source_name')

    config.table_rule_configs = load_table_rule_configs(cfg.get('tables'))
    config.binding_tables_groups = cfg.get('binding_tables')

    config.default_database_sharding_strategy_config = cfg.get('default_database_strategy')
    config.default_table_sharding_strategy_config = cfg.get('default_database_strategy')

    config.master_slave_rule_configs = load_master_slave_rule_config(cfg.get('master_slave_rules'))

    config.default_key_generator = cfg['default_key_generator']
    config.config_map = cfg.get('config_map')
    config.props = cfg.get('props')
    return config


def load_table_rule_configs(cfg):
    table_rule_configs = list()
    for table_logic_name, _table_rule_config in cfg.items():
        table_rule_config = TableRuleConfiguration()
        table_rule_config.logic_table = table_logic_name
        table_rule_config.actual_data_nodes = _table_rule_config['actual_data_nodes']
        table_rule_config.database_strategy_config = _table_rule_config.get('database_strategy')
        table_rule_config.table_strategy_config = _table_rule_config.get('table_strategy')
        table_rule_config.key_generator_name = _table_rule_config.get('key_generator_name')
        table_rule_config.key_generator = _table_rule_config.get('key_generator')
        table_rule_config.logic_index = _table_rule_config.get('logic_index')
        table_rule_configs.append(table_rule_config)
    return table_rule_configs


def load_master_slave_rule_config(cfg):
    result = list()
    if not cfg:
        return result
    for name, ms in cfg.items():
        master_slave_rule_config = dict()
        master_slave_rule_config['name'] = name
        master_slave_rule_config['master_data_source_name'] = ms['master_data_source_name']
        master_slave_rule_config['slave_data_source_names'] = ms['slave_data_source_names']
        master_slave_rule_config['load_balance_algorithm'] = ms['load_balance_algorithm']
        result.append(master_slave_rule_config)
    return result