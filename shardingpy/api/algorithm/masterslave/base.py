import enum

from shardingpy.api.algorithm.masterslave.impl import RoundRobinMasterSlaveLoadBalanceAlgorithm, \
    RandomMasterSlaveLoadBalanceAlgorithm


class MasterSlaveLoadBalanceAlgorithm:
    def get_data_source(self, name, master_data_source_name, slave_data_source_names):
        """
        get data source for execute sql
        :param name:
        :param master_data_source_name:
        :param slave_data_source_names:
        :return: data source name
        """
        raise NotImplementedError


class MasterSlaveLoadBalanceAlgorithmType(enum.Enum):
    ROUND_ROBIN = 'ROUND_ROBIN'
    RANDOM = 'RANDOM'


def get_master_slave_load_balance_algorithm_by_type(algorithm_type):
    assert isinstance(algorithm_type, MasterSlaveLoadBalanceAlgorithmType)
    return RandomMasterSlaveLoadBalanceAlgorithm() if algorithm_type == MasterSlaveLoadBalanceAlgorithmType.RANDOM \
        else RoundRobinMasterSlaveLoadBalanceAlgorithm()


def get_default_master_slave_load_balance_algorithm():
    return RoundRobinMasterSlaveLoadBalanceAlgorithm()
