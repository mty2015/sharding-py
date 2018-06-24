from shardingpy.api.algorithm.sharding.base import ComplexKeysShardingAlgorithm


class TestComplexKeysShardingAlgorithm(ComplexKeysShardingAlgorithm):
    def do_sharding(self, available_target_names, sharding_values):
        return available_target_names
