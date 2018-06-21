sharding_rule_config = {
    'data_sources': {
        'master_ds_0': None,
        'master_ds_0_slave_0': None,
        'master_ds_0_slave_1': None,
        'master_ds_1': None,
        'master_ds_1_slave_0': None,
        'master_ds_1_slave_1': None,
        'default_ds': None
    },
    'sharding_rule': {
        'tables': {
            't_user': {
                'actual_data_nodes': 'ds_${0..1}.t_user_${0..15}',
                'database_strategy': {
                    'complex': {
                        'sharding_columns': ['region_id', 'user_id'],
                        'algorithm': None
                    }
                },
                'table_strategy': {
                    'complex': {
                        'sharding_columns': ['region_id', 'user_id'],
                        'algorithm': None
                    }
                }
            },

            't_stock': {
                'actual_data_nodes': 'ds_${0..1}.t_stock{0..8}',
                'database_strategy': {
                    'hint': {
                        'algorithm': None
                    }
                },
                'table_strategy': {
                    'hint': {
                        'algorithm': None
                    }
                }
            },

            't_order': {
                'actual_data_nodes': 'ds_${0..1}.t_order_${0..1}',
                'table_strategy': {
                    'hint': {
                        'sharding_columns': ['order_id'],
                        'algorithm_expression': 't_order_${order_id % 2}'
                    }
                },
                'key_generator_column_name': 'order_id',
                'key_generator': None,
                'logic_index': 'order_index'
            },

            't_order_item': {
                'actual_data_nodes': 'ds_${0..1}.t_order_item_${0..1}',
                'table_strategy': {
                    'standard': {
                        'sharding_columns': ['order_id'],
                        'precise_algorithm': None,
                        'range_algorith': None
                    }
                },
                'key_generator_column_name': 'order_id',
                'key_generator': None,
                'logic_index': 'order_index'
            }
        },
        'binding_tables': [('t_order', 't_order_item')],
        'default_data_source_name': 'default_ds',
        'default_database_strategy': {
            'inline': {
                'sharding_columns': ['order_id'],
                'algorithm_expression': 'ds_${order_id % 2}',
            }
        },

        'default_table_strategy': None,

        'default_key_generator': None,

        'master_slave_rules': {
            'ds_0': {
                'master_data_source_name': 'master_ds_0',
                'slave_data_source_names': ['master_ds_0_slave_0', 'master_ds_0_slave_0'],
                'load_balance_algorithm_type': 'ROUND_ROBIN',
                'config_map': {
                    'master-slave-key0': 'master-slave-key0'
                }
            },
            'ds_1': {
                'master_data_source_name': 'master_ds_1',
                'slave_data_source_names': ['master_ds_1_slave_0', 'master_ds_1_slave_0'],
                'load_balance_algorithm': None,
                'config_map': {
                    'master-slave-key1': 'master-slave-key1'
                }
            }
        },

        'config_map': {
            'sharding_key1': 'sharding_value1'
        },

        'props': {
            'sql.show': True
        }
    }
}
