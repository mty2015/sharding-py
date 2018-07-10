sharding_rule_config = {
    'data_sources': {
        'ds0': None,
        'ds1': None
    },
    'sharding_rule': {
        'tables': {
            't_order': {
                'actual_data_nodes': ['ds0.t_order0', 'ds0.t_order1', 'ds1.t_order0', 'ds1.t_order1'],
                'database_strategy': {
                    'complex': {
                        'sharding_columns': ['user_id'],
                        'algorithm_class_name': 'tests.api.algorithm.fixture.TestComplexKeysShardingAlgorithm'
                    }
                },
                'table_strategy': {
                    'complex': {
                        'sharding_columns': ['order_id'],
                        'algorithm_class_name': 'tests.api.algorithm.fixture.TestComplexKeysShardingAlgorithm'
                    }
                },
                'key_generator_column_name': 'order_id',
                'key_generator_class_name': 'shardingpy.keygen.base.DefaultKeyGenerator',
                'logic_index': 'order_index'
            }
        }
    }
}
