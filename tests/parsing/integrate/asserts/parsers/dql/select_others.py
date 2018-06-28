parser_result_set = {
    "assertSelectInWithNullParameter": {
        "parameters": (1, 'null'),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 14},
                ],
            },
        "order_by_columns": [
            {"name": "order_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectPaginationWithRowCount": {
        "parameters": (1, 2, 9, 10, 5),
        "tables": [
            {"name": "t_order", "alias": "o"},
            {"name": "t_order_item", "alias": "i"},
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 16},
                    {"original_literals": "t_order_item", "begin_position": 31},
                ],
                "row_count_token": {"begin_position": 187, "row_count": 5}
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN",
                 "values": [(0, 1), (1, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN",
                 "values": [(2, 9), (3, 10)]},
            ]
        ],
        "order_by_columns": [
            {"name": "item_id", "owner": "i", "order_direction": "DESC"}
        ],
        "limit": {"row_count": 5, "row_count_index": 4}
    }
}
