parser_result_set = {
    "assertSelectSum": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 40}
                ]
            },
        "aggregation_select_items": [
            {"type": "SUM", "inner_expression": "(user_id)", "alias": "user_id_sum"}
        ]
    },
    "assertSelectCount": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 37}
                ]
            },
        "aggregation_select_items": [
            {"type": "COUNT", "inner_expression": "(*)", "alias": "orders_count"}
        ]
    },
    "assertSelectMax": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 40}
                ]
            },
        "aggregation_select_items": [
            {"type": "MAX", "inner_expression": "(user_id)", "alias": "max_user_id"}
        ]
    },
    "assertSelectMin": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 40}
                ]
            },
        "aggregation_select_items": [
            {"type": "MIN", "inner_expression": "(user_id)", "alias": "min_user_id"}
        ]
    },
    "assertSelectAvg": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 40}
                ],
                "items_token": {
                    "begin_position": 35,
                    "items": ["COUNT(user_id) AS AVG_DERIVED_COUNT_0 ", "SUM(user_id) AS AVG_DERIVED_SUM_0 "]
                }
            },
        "aggregation_select_items": [
            {
                "type": "AVG", "inner_expression": "(user_id)", "alias": "user_id_avg",
                "derived_columns": [
                    {"type": "COUNT", "inner_expression": "(user_id)", "alias": "AVG_DERIVED_COUNT_0"},
                    {"type": "SUM", "inner_expression": "(user_id)", "alias": "AVG_DERIVED_SUM_0"}
                ]
            }
        ]
    },
    "assertSelectCountForSpecialSymbol": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 46}
                ]
            },
        "aggregation_select_items": [
            {"type": "COUNT", "inner_expression": "(`order_id`)", "alias": "orders_count"}
        ]
    },
}
