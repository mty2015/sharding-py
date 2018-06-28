parser_result_set = {
    "assertSelectWithOrderBy": {
        "tables": [
            {"name": "t_order", "alias": "o"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 14},
                ],
            },
        "order_by_columns": [
            {"name": "order_id", "owner": "o", "order_direction": "ASC"},
            {"index": 2, "order_direction": "DESC"}
        ]
    },
    "assertSelectWithOrderByForIndex": {
        "tables": [
            {"name": "t_order", "alias": "o"},
            {"name": "t_order_item", "alias": "i"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 16},
                    {"original_literals": "t_order_item", "begin_position": 27},
                ],
            },
        "order_by_columns": [
            {"name": "order_id", "owner": "o", "order_direction": "DESC"},
            {"index": 1, "order_direction": "ASC"}
        ]
    },
}
