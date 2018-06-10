class NoneShardingStrategy(object):
    def do_sharding(self, available_target_names, sharding_values):
        return []


def get_sharding_strategy(sharding_strategy_config):
    pass
