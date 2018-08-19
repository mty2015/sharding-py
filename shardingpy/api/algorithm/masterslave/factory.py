from shardingpy.api.algorithm.masterslave.base import MasterSlaveLoadBalanceAlgorithmType
from shardingpy.api.algorithm.masterslave.impl import RandomMasterSlaveLoadBalanceAlgorithm, \
    RoundRobinMasterSlaveLoadBalanceAlgorithm


def get_master_slave_load_balance_algorithm_by_type(algorithm_type):
    assert isinstance(algorithm_type, MasterSlaveLoadBalanceAlgorithmType)
    return RandomMasterSlaveLoadBalanceAlgorithm() if algorithm_type == MasterSlaveLoadBalanceAlgorithmType.RANDOM \
        else RoundRobinMasterSlaveLoadBalanceAlgorithm()


def get_default_master_slave_load_balance_algorithm():
    return RoundRobinMasterSlaveLoadBalanceAlgorithm()
