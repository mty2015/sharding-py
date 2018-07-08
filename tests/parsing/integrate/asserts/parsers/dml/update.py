parser_result_set = {
    "assertUpdateWithAlias": {
        "parameters": ("'update'", 1, 1),
        "tables": [
            {"name": "t_order", "alias": "o"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 7}
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "order_id", "operator": "EQUAL", "values": [(1, 1)]},
                {"table_name": "t_order", "column_name": "user_id", "operator": "EQUAL", "values": [(2, 1)]},
            ]
        ]
    },
    "assertUpdateWithoutAlias": {
        "parameters": ("'update'", 1, 1),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 7}
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "order_id", "operator": "EQUAL", "values": [(1, 1)]},
                {"table_name": "t_order", "column_name": "user_id", "operator": "EQUAL", "values": [(2, 1)]},
            ]
        ]
    }
}
