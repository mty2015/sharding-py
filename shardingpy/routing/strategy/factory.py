from shardingpy.api.config.base import ComplexShardingStrategyConfiguration
from shardingpy.routing.strategy.complex import ComplexShardingStrategy


def get_sharding_strategy(sharding_strategy_config):
    if isinstance(sharding_strategy_config, ComplexShardingStrategyConfiguration):
        return ComplexShardingStrategy(sharding_strategy_config)