parser_result_set = {
    "assertSelectDateFuncWithGroupBy": {
        "parameters": (1000, 1100),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "`t_order_item`", "begin_position": 76},
                ],
                "order_by_token": {"placeholder_begin_position": 156, "literal_begin_position": 162},
            },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "order_id", "operator": "IN",
                 "values": [(0, 1000), (1, 1100)]}
            ]
        ],
        "aggregation_select_items": [
            {"type": "COUNT", "inner_expression": "(*)", "alias": "c_number"}
        ],
        "group_by_columns": [
            {"name": "date_format(c_date, '%y-%m-%d')", "alias": "c_date", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "date_format(c_date, '%y-%m-%d')", "alias": "c_date", "order_direction": "ASC"}
        ]
    },
    "assertSelectSumWithGroupBy": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 49},
                ]
            },
        "aggregation_select_items": [
            {"type": "SUM", "inner_expression": "(order_id)", "alias": "orders_sum"}
        ],
        "group_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectCountWithGroupBy": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 53},
                ]
            },
        "aggregation_select_items": [
            {"type": "COUNT", "inner_expression": "(order_id)", "alias": "orders_count"}
        ],
        "group_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectMaxWithGroupBy": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 51},
                ]
            },
        "aggregation_select_items": [
            {"type": "MAX", "inner_expression": "(order_id)", "alias": "max_order_id"}
        ],
        "group_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectMinWithGroupBy": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 51},
                ]
            },
        "aggregation_select_items": [
            {"type": "MIN", "inner_expression": "(order_id)", "alias": "min_order_id"}
        ],
        "group_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectAvgWithGroupBy": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 49},
                ],
                "items_token": {
                    "begin_position": 44,
                    "items": ["COUNT(order_id) AS AVG_DERIVED_COUNT_0 ", "SUM(order_id) AS AVG_DERIVED_SUM_0 "]
                }
            },
        "aggregation_select_items": [
            {
                "type": "AVG", "inner_expression": "(order_id)", "alias": "orders_avg",
                "derived_columns": [
                    {"type": "COUNT", "inner_expression": "(order_id)", "alias": "AVG_DERIVED_COUNT_0"},
                    {"type": "SUM", "inner_expression": "(order_id)", "alias": "AVG_DERIVED_SUM_0"}
                ]
            }
        ],
        "group_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectOrderByDescWithGroupBy": {
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 49},
                ]
            },
        "aggregation_select_items": [
            {"type": "SUM", "inner_expression": "(order_id)", "alias": "orders_sum"}
        ],
        "group_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "orders_sum", "alias": "orders_sum", "order_direction": "DESC"}
        ]
    },
    "assertSelectCountWithoutGroupedColumn": {
        "parameters": (1, 2, 9, 10),
        "tables": [
            {"name": "t_order", "alias": "o"},
            {"name": "t_order_item", "alias": "i"},
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 36},
                    {"original_literals": "t_order_item", "begin_position": 51},
                ],
                "items_token": {
                    "begin_position": 31,
                    "items": ["o.user_id AS GROUP_BY_DERIVED_0 "]
                },
                "order_by_token": {"placeholder_begin_position": 194, "literal_begin_position": 195},
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN",
                 "values": [(0, 1), (1, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN",
                 "values": [(2, 9), (3, 10)]},
            ]
        ],
        "aggregation_select_items": [
            {
                "type": "COUNT", "inner_expression": "(*)", "alias": "items_count",
            }
        ],
        "group_by_columns": [
            {"name": "user_id", "alias": "GROUP_BY_DERIVED_0", "owner": "o", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "user_id", "alias": "GROUP_BY_DERIVED_0", "owner": "o", "order_direction": "ASC"}
        ]
    },
    "assertSelectCountWithGroupByBindingTable": {
        "parameters": (1, 2, 9, 10),
        "tables": [
            {"name": "t_order", "alias": "o"},
            {"name": "t_order_item", "alias": "i"},
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 47},
                    {"original_literals": "t_order_item", "begin_position": 62},
                ],
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN",
                 "values": [(0, 1), (1, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN",
                 "values": [(2, 9), (3, 10)]},
            ]
        ],
        "aggregation_select_items": [
            {
                "type": "COUNT", "inner_expression": "(*)", "alias": "items_count",
            }
        ],
        "group_by_columns": [
            {"name": "user_id", "owner": "o", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "user_id", "owner": "o", "order_direction": "ASC"}
        ]
    },
    "assertSelectWithGroupByAndLimit": {
        "parameters": (5,),
        "tables": [
            {"name": "t_order"},
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 20},
                ],
                "row_count_token": {"begin_position": 68, "row_count": 5},
            },
        "group_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ],
        "limit": {"row_count_index": 0, "row_count": 5}
    },
    "assertSelectWithGroupByAndOrderByAndLimit": {
        "parameters": (5,),
        "tables": [
            {"name": "t_order"},
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 35},
                ],
                "row_count_token": {"begin_position": 89, "row_count": 5},
            },
        "aggregation_select_items": [
            {
                "type": "SUM", "inner_expression": "(order_id)",
            }
        ],
        "group_by_columns": [
            {"name": "user_id", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "SUM(order_id)", "order_direction": "ASC"}
        ],
        "limit": {"row_count_index": 0, "row_count": 5}
    },
    "assertSelectItemWithAliasAndMatchOrderByAndGroupByItems": {
        "tables": [
            {"name": "t_order", "alias": "o"},
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 26},
                ],
            },
        "group_by_columns": [
            {"name": "user_id", "owner": "o", "alias": "uid", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "user_id", "owner": "o", "alias": "uid", "order_direction": "ASC"}
        ]
    },
    "assertSelectGroupByWithAliasIsKeyword": {
        "tables": [
            {"name": "t_order"},
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 58},
                ],
                "order_by_token": {"placeholder_begin_position": 80, "literal_begin_position": 80},
            },
        "aggregation_select_items": [
            {
                "type": "SUM", "inner_expression": "(order_id)", "alias": "orders_sum"
            }
        ],
        "group_by_columns": [
            {"name": "key", "alias": "key", "order_direction": "ASC"}
        ],
        "order_by_columns": [
            {"name": "key", "alias": "key", "order_direction": "ASC"}
        ]
    },
}
