sharding_rule_config = {
    'data_sources': {
        'ds0': None,
        'ds1': None
    },
    'sharding_rule': {
        'tables': {
            'table_x': {
                'actual_data_nodes': ['ds0.table_x', 'ds1.table_x'],
                'key_generator_column_name': 'id',
                'logic_index': 'logic_index'
            },
            'table_y': {
                'actual_data_nodes': ['ds0.table_y', 'ds1.table_y'],
                'logic_index': 'logic_index'
            }
        },
        'binding_tables': [('table_x', 'table_y')]
    }
}
