class NoneShardingStrategy(object):
    def __init__(self):
        self.sharding_columns = list()

    def do_sharding(self, available_target_names, sharding_values):
        return available_target_names


def get_sharding_strategy(sharding_strategy_config):
    pass
