sharding_rule_config = {
    'data_sources': {
        'ds0': None,
        'ds1': None
    },
    'sharding_rule': {
        'tables': {
            't_order': {
                'actual_data_nodes': ['ds0.t_order', 'ds0.t_order'],
                'table_strategy': {
                    'complex': {
                        'sharding_columns': ['user_id', 'order_id'],
                        'algorithm': None
                    }
                },
                'logic_index': 'order_index'
            },
            't_order_item': {
                'actual_data_nodes': ['ds0.t_order_item', 'ds1.t_order_item'],
                'table_strategy': {
                    'complex': {
                        'sharding_columns': ['user_id', 'order_id', 'item_id'],
                        'algorithm': None
                    }
                },
                'key_generator_column_name': 'item_id'
            },
            't_place': {
                'actual_data_nodes': ['db0.t_place', 'db1.t_place'],
                'table_strategy': {
                    'complex': {
                        'sharding_columns': ['user_new_id', 'guid'],
                        'algorithm': None
                    }
                },
                'key_generator_column_name': 'item_id'
            }
        },
        'binding_tables': [('t_order', 't_order_item')],
        'default_key_generator': None
    }
}
