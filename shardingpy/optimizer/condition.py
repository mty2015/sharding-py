class ShardingCondition:
    def __init__(self):
        self.sharding_values = list()


class AlwaysFalseShardingCondition(ShardingCondition):
    pass


class ShardingConditions:
    def __init__(self, sharding_conditions):
        self.sharding_conditions = sharding_conditions

    def is_always_false(self):
        if len(self.sharding_conditions) == 0:
            return False
        for each in self.sharding_conditions:
            if not isinstance(each, AlwaysFalseShardingCondition):
                return False
        return True
