from .base import ShardingStrategy


class ComplexShardingStrategy(ShardingStrategy):
    def __init__(self, strategy_config):
        self._sharding_columns = list()
        self._sharding_columns.extend(strategy_config.sharding_columns)
        self._sharding_sharding_algorithm = strategy_config.sharding_algorithm

    def get_sharding_columns(self):
        return self._sharding_columns

    def do_sharding(self, available_target_names, sharding_values):
        return self._sharding_sharding_algorithm.do_sharding(available_target_names, sharding_values)
