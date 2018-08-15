import random

from shardingpy.api.algorithm.masterslave.base import MasterSlaveLoadBalanceAlgorithm


class RandomMasterSlaveLoadBalanceAlgorithm(MasterSlaveLoadBalanceAlgorithm):
    def get_data_source(self, name, master_data_source_name, slave_data_source_names):
        return random.choice(slave_data_source_names)


class RoundRobinMasterSlaveLoadBalanceAlgorithm(MasterSlaveLoadBalanceAlgorithm):
    COUNT_MAP = dict()

    def get_data_source(self, name, master_data_source_name, slave_data_source_names):
        count = self.COUNT_MAP.get(name, 0)
        if count >= len(slave_data_source_names):
            count = 0
        self.COUNT_MAP[name] = count
        return slave_data_source_names[count % len(slave_data_source_names)]
