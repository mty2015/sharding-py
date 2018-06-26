parser_result_set = {
    "assertSelectOne": {

    },
    "assertSelectNotEqualsWithSingleTable": {
        "parameters": (1,),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order_item",
                        "begin_position": 14
                    }
                ]
            },
        "order_by_columns": [
            {"name": "item_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectNotEqualsWithSingleTableForExclamationEqual": {
        "parameters": (1,),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order_item",
                        "begin_position": 14
                    }
                ]
            },
        "order_by_columns": [
            {"name": "item_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectNotEqualsWithSingleTableForNotIn": {
        "parameters": (100000, 100001),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order_item",
                        "begin_position": 14
                    }
                ]
            },
        "order_by_columns": [
            {"name": "item_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectNotEqualsWithSingleTableForNotBetween": {
        "parameters": (100000, 100001),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order_item",
                        "begin_position": 14
                    }
                ]
            },
        "order_by_columns": [
            {"name": "item_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectEqualsWithSingleTable": {
        "parameters": (1, 1),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order",
                        "begin_position": 14
                    }
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "EQUAL", "values": [(0, 1)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "EQUAL", "values": [(1, 1)]}
            ]
        ]
    },
    "assertSelectEqualsWithSameShardingColumns": {
        "parameters": (1, 2),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order",
                        "begin_position": 14
                    }
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "order_id", "operator": "EQUAL", "values": [(0, 1)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "EQUAL", "values": [(1, 2)]}
            ]
        ]
    },
    "assertSelectBetweenWithSingleTable": {
        "parameters": (1, 10, 2, 5),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order",
                        "begin_position": 14
                    }
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "BETWEEN", "values": [(0, 1), (1, 10)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN", "values": [(2, 2), (3, 5)]}
            ]
        ],
        "order_by_columns": [
            {"name": "user_id", "order_direction": "ASC"},
            {"name": "order_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectInWithSingleTable": {
        "parameters": (1, 2, 3, 9, 10),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order",
                        "begin_position": 14
                    }
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN",
                 "values": [(0, 1), (1, 2), (2, 3)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "IN", "values": [(3, 9), (4, 10)]}
            ]
        ],
        "order_by_columns": [
            {"name": "user_id", "order_direction": "ASC"},
            {"name": "order_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectInWithSameShardingColumns": {
        "parameters": (100, 1001, 1001, 1003),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order",
                        "begin_position": 14
                    }
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "order_id", "operator": "IN", "values": [(0, 100), (1, 1001)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "IN", "values": [(2, 1001), (3, 1003)]}
            ]
        ],
        "order_by_columns": [
            {"name": "order_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectIterator": {
        "parameters": (1, 2),
        "tables": [
            {"name": "t_order_item", "alias": "t"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {
                        "original_literals": "t_order_item",
                        "begin_position": 16
                    }
                ]
            },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "item_id", "operator": "IN", "values": [(0, 1), (1, 2)]}
            ]
        ]
    },
    "assertSelectNoShardingTable": {
        "tables": [
            {"name": "t_order", "alias": "o"},
            {"name": "t_order_item", "alias": "i"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 16},
                    {"original_literals": "t_order_item", "begin_position": 31}
                ]
            },
        "order_by_columns": [
            {"owner": "i", "name": "item_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectLikeWithCount": {
        "parameters": ("'init'", 1, 2, 9, 10),
        "tables": [
            {"name": "t_order", "alias": "o"}
        ],
        "tokens": {
            "table_tokens": [
                {"original_literals": "`t_order`", "begin_position": 37}
            ]
        },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN", "values": [(1, 1), (2, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN", "values": [(3, 9), (4, 10)]}
            ]
        ],
        "aggregation_select_items": [
            {"type": "COUNT", "inner_expression": "(0)", "alias": "orders_count"}
        ]
    },
    "assertSelectWithBindingTable": {
        "parameters": (1, 2, 9, 10),
        "tables": [
            {"name": "t_order", "alias": "o"},
            {"name": "t_order_item", "alias": "i"}
        ],
        "tokens": {
            "table_tokens": [
                {"original_literals": "t_order", "begin_position": 16},
                {"original_literals": "t_order_item", "begin_position": 31}
            ]
        },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN", "values": [(0, 1), (1, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN", "values": [(2, 9), (3, 10)]}
            ]
        ],
        "order_by_columns": [
            {"owner": "i", "name": "item_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectWithBindingTableAndConfigTable": {
        "parameters": (1, 2, 9, 10, 'init'),
        "tables": [
            {"name": "t_order", "alias": "o"},
            {"name": "t_order_item", "alias": "i"}
        ],
        "tokens": {
            "table_tokens": [
                {"original_literals": "t_order", "begin_position": 16},
                {"original_literals": "t_order_item", "begin_position": 31}
            ]
        },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN", "values": [(0, 1), (1, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN", "values": [(2, 9), (3, 10)]}
            ]
        ],
        "order_by_columns": [
            {"owner": "i", "name": "item_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectWithUpperCaseBindingTable": {
        "parameters": (1, 2, 9, 10),
        "tables": [
            {"name": "T_ORDER", "alias": "o"},
            {"name": "T_order_item", "alias": "i"}
        ],
        "tokens": {
            "table_tokens": [
                {"original_literals": "T_ORDER", "begin_position": 16},
                {"original_literals": "T_order_item", "begin_position": 31}
            ]
        },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN", "values": [(0, 1), (1, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN", "values": [(2, 9), (3, 10)]}
            ]
        ],
        "order_by_columns": [
            {"owner": "i", "name": "item_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectWithUpperCaseBindingTableAndConfigTable": {
        "parameters": (1, 2, 9, 10, 'init'),
        "tables": [
            {"name": "T_ORDER", "alias": "o"},
            {"name": "T_order_item", "alias": "i"}
        ],
        "tokens": {
            "table_tokens": [
                {"original_literals": "T_ORDER", "begin_position": 34},
                {"original_literals": "T_order_item", "begin_position": 49}
            ]
        },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN", "values": [(0, 1), (1, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN", "values": [(2, 9), (3, 10)]}
            ]
        ],
        "order_by_columns": [
            {"owner": "i", "name": "item_id", "order_direction": "ASC"}
        ]
    },
    "assertSelectCountWithBindingTable": {
        "parameters": (1, 2, 9, 10),
        "tables": [
            {"name": "t_order", "alias": "o"},
            {"name": "t_order_item", "alias": "i"}
        ],
        "tokens": {
            "table_tokens": [
                {"original_literals": "t_order", "begin_position": 36},
                {"original_literals": "t_order_item", "begin_position": 47}
            ]
        },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN", "values": [(0, 1), (1, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN", "values": [(2, 9), (3, 10)]}
            ]
        ],
        "aggregation_select_items": [
            {"type": "COUNT", "inner_expression": "(*)", "alias": "items_count"}
        ]
    },
    "assertSelectCountWithBindingTableWithJoin": {
        "parameters": (1, 2, 9, 10),
        "tables": [
            {"name": "t_order", "alias": "o"},
            {"name": "t_order_item", "alias": "i"}
        ],
        "tokens": {
            "table_tokens": [
                {"original_literals": "t_order", "begin_position": 36},
                {"original_literals": "t_order_item", "begin_position": 51}
            ]
        },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "user_id", "operator": "IN", "values": [(0, 1), (1, 2)]},
                {"table_name": "t_order", "column_name": "order_id", "operator": "BETWEEN", "values": [(2, 9), (3, 10)]}
            ]
        ],
        "aggregation_select_items": [
            {"type": "COUNT", "inner_expression": "(*)", "alias": "items_count"}
        ]
    },
    "assertSelectAliasWithKeyword": {
        "parameters": (1,),
        "tables": [
            {"name": "t_order_item", "alias": "length"}
        ],
        "tokens": {
            "table_tokens": [
                {"original_literals": "t_order_item", "begin_position": 36}
            ]
        },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "item_id", "operator": "EQUAL", "values": [(0, 1)]},
            ]
        ]
    },
}
