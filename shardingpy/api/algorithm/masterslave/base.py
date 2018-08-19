import enum


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
