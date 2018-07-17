sharding_rule_config = {
    'data_sources': {
        'db0': None,
        'db1': None
    },
    'sharding_rule': {
        'tables': {
            'table_x': {
                'actual_data_nodes': ['db0.table_x', 'db1.table_x'],
                'key_generator_column_name': 'id',
                'logic_index': 'logic_index'
            },
            'table_y': {
                'actual_data_nodes': ['db0.table_y', 'db1.table_y'],
                'logic_index': 'logic_index'
            }
        },
        'binding_tables': [('table_x', 'table_y')]
    }
}
