parser_result_set = {
    "assertSelectExpressionWithSingleTable": {
        "tables": [
            {"name": "t_order", "alias": "o"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 31}
                ],
                "items_token": {
                    "begin_position": 26,
                    "items": ["o.order_id AS ORDER_BY_DERIVED_0 "]
                }
            },
        "order_by_columns": [
            {"owner": "o", "name": "order_id", "alias": "ORDER_BY_DERIVED_0", "order_direction": "ASC"}
        ]
    },
    "assertSelectDateFuncWithSingleTable": {
        "tables": [
            {"name": "t_order_item", "alias": "i"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "`t_order_item`", "begin_position": 37}
                ]
            },
        "order_by_columns": [
            {"name": "DATE(i.c_date)", "alias": "c_date", "order_direction": "DESC"}
        ]
    },
    "assertSelectCountWithExpression": {
        "tables": [
            {"name": "t_order", "alias": "o"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 36}
                ]
            },
        "aggregation_select_items": [
            {"type": "COUNT", "inner_expression": "(o.order_id)"}
        ]
    },
    "assertSelectRegexpWithSingleTable": {
        "parameters": ('init', 1, 2),
        "tables": [
            {"name": "t_order_item", "alias": "t"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order_item", "begin_position": 14}
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "item_id", "operator": "IN", "values": [(1, 1), (2, 2)]},
            ]
        ],
    },
}
