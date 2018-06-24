class ShardingStrategy:
    def get_sharding_columns(self):
        raise NotImplementedError()

    def do_sharding(self, available_target_names, sharding_values):
        raise NotImplementedError()


class NoneShardingStrategy(ShardingStrategy):

    def __init__(self):
        self._sharding_columns = list()

    def get_sharding_columns(self):
        return self._sharding_columns

    def do_sharding(self, available_target_names, sharding_values):
        return available_target_names


