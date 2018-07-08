parser_result_set = {
    "assertDeleteWithShardingValue": {
        "parameters": (1000, 1001, "'init'"),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 12}
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "order_id", "operator": "EQUAL", "values": [(0, 1000)]},
                {"table_name": "t_order", "column_name": "user_id", "operator": "EQUAL", "values": [(1, 1001)]},
            ]
        ]
    },
    "assertDeleteWithoutShardingValue": {
        "parameters": ("'init'", ),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 12}
                ]
            }
    }
}
