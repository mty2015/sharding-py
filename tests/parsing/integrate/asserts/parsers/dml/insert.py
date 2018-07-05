parser_result_set = {
    "assertInsertWithAllPlaceholders": {
        "parameters": (1, 1, 'init'),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 12}
                ],
                "insert_values_token": {"begin_position": 55, "table_name": "t_order"}
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "order_id", "operator": "EQUAL", "values": [(0, 1)]},
                {"table_name": "t_order", "column_name": "user_id", "operator": "EQUAL", "values": [(1, 1)]},
            ]
        ]
    }
}
