parser_result_set = {
    "assertInsertWithAllPlaceholders": {
        "parameters": (1, 1, "'init'"),
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
    },
    "assertInsertWithPartialPlaceholder": {
        "parameters": (1, 1),
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
    },
    "assertInsertWithGenerateKeyColumn": {
        "parameters": (10000, 1000, 10),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order_item", "begin_position": 12}
                ],
                "insert_values_token": {"begin_position": 68, "table_name": "t_order_item"}
            },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "item_id", "operator": "EQUAL", "values": [(0, 10000)]},
                {"table_name": "t_order_item", "column_name": "order_id", "operator": "EQUAL", "values": [(1, 1000)]},
                {"table_name": "t_order_item", "column_name": "user_id", "operator": "EQUAL", "values": [(2, 10)]},
            ]
        ]
    },
    "assertInsertWithoutGenerateKeyColumn": {
        "parameters": (1000, 10),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order_item", "begin_position": 12}
                ],
                "items_token": {
                    "begin_position": 50,
                    "items": ["item_id"]
                },
                "insert_values_token": {"begin_position": 59, "table_name": "t_order_item"}
            },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "order_id", "operator": "EQUAL", "values": [(0, 1000)]},
                {"table_name": "t_order_item", "column_name": "user_id", "operator": "EQUAL", "values": [(1, 10)]},
            ]
        ]
    },
    "assertInsertOnDuplicateKeyUpdate": {
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
    },
    "assertBatchInsertWithGenerateKeyColumn": {
        "parameters": (10000, 1000, 10, 10010, 1001, 10),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order_item", "begin_position": 12}
                ],
                "insert_values_token": {"begin_position": 68, "table_name": "t_order_item"}
            },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "item_id", "operator": "EQUAL", "values": [(0, 10000)]},
                {"table_name": "t_order_item", "column_name": "order_id", "operator": "EQUAL", "values": [(1, 1000)]},
                {"table_name": "t_order_item", "column_name": "user_id", "operator": "EQUAL", "values": [(2, 10)]},
            ],
            [
                {"table_name": "t_order_item", "column_name": "item_id", "operator": "EQUAL", "values": [(3, 10010)]},
                {"table_name": "t_order_item", "column_name": "order_id", "operator": "EQUAL", "values": [(4, 1001)]},
                {"table_name": "t_order_item", "column_name": "user_id", "operator": "EQUAL", "values": [(5, 10)]},
            ]
        ]
    },
    "assertBatchInsertWithoutGenerateKeyColumn": {
        "parameters": (1000, 10, 1001, 10),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order_item", "begin_position": 12}
                ],
                "insert_values_token": {"begin_position": 59, "table_name": "t_order_item"},
                "items_token": {"begin_position": 50, "items": ["item_id"]},
            },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "order_id", "operator": "EQUAL", "values": [(0, 1000)]},
                {"table_name": "t_order_item", "column_name": "user_id", "operator": "EQUAL", "values": [(1, 10)]},
            ],
            [
                {"table_name": "t_order_item", "column_name": "order_id", "operator": "EQUAL", "values": [(2, 1001)]},
                {"table_name": "t_order_item", "column_name": "user_id", "operator": "EQUAL", "values": [(3, 10)]},
            ]
        ]
    },
    "assertInsertWithJsonAndGeo": {
        "parameters": (7, 200, 100, 200, """'{"rule":"null"}'"""),
        "tables": [
            {"name": "t_place"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_place", "begin_position": 12}
                ],
                "insert_values_token": {"begin_position": 64, "table_name": "t_place"}
            },
        "or_condition": [
            [
                {"table_name": "t_place", "column_name": "user_new_id", "operator": "EQUAL", "values": [(0, 7)]},
                {"table_name": "t_place", "column_name": "guid", "operator": "EQUAL", "values": [(1, 200)]},
            ]
        ]
    },
    "assertInsertWithoutColumnsWithAllPlaceholders": {
        "parameters": (1, 1, "'init'"),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 12}
                ],
                "insert_column_token": {"begin_position": 19, "column_name": "("},
                "items_token": {"begin_position": 19, "items": ["order_id", "user_id"]},
                "insert_column_token": {"begin_position": 19, "column_name": ")"},
                "insert_values_token": {"begin_position": 27, "table_name": "t_order"}
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "order_id", "operator": "EQUAL", "values": [(0, 1)]},
                {"table_name": "t_order", "column_name": "user_id", "operator": "EQUAL", "values": [(1, 1)]},
            ]
        ]
    },
    "assertInsertWithoutColumnsWithPartialPlaceholder": {
        "parameters": (1, 1),
        "tables": [
            {"name": "t_order"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order", "begin_position": 12}
                ],
                "insert_column_token": {"begin_position": 19, "column_name": "("},
                "items_token": {"begin_position": 19, "items": ["order_id", "user_id"]},
                "insert_column_token": {"begin_position": 19, "column_name": ")"},
                "insert_values_token": {"begin_position": 27, "table_name": "t_order"}
            },
        "or_condition": [
            [
                {"table_name": "t_order", "column_name": "order_id", "operator": "EQUAL", "values": [(0, 1)]},
                {"table_name": "t_order", "column_name": "user_id", "operator": "EQUAL", "values": [(1, 1)]},
            ]
        ]
    },
    "assertInsertWithoutColumnsWithGenerateKeyColumn": {
        "parameters": (10000, 1000, 10),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order_item", "begin_position": 12}
                ],
                "insert_column_token": {"begin_position": 24, "column_name": "("},
                "items_token": {"begin_position": 24, "items": ["item_id", "order_id", "user_id", "status", "c_date"]},
                "insert_column_token": {"begin_position": 24, "column_name": ")"},
                "insert_values_token": {"begin_position": 31, "table_name": "t_order_item"}
            },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "item_id", "operator": "EQUAL", "values": [(0, 10000)]},
                {"table_name": "t_order_item", "column_name": "order_id", "operator": "EQUAL", "values": [(1, 1000)]},
                {"table_name": "t_order_item", "column_name": "user_id", "operator": "EQUAL", "values": [(2, 10)]},
            ]
        ]
    },
    "assertInsertWithoutColumnsWithoutGenerateKeyColumn": {
        "parameters": (1000, 10),
        "tables": [
            {"name": "t_order_item"}
        ],
        "tokens":
            {
                "table_tokens": [
                    {"original_literals": "t_order_item", "begin_position": 12}
                ],
                "insert_column_token": {"begin_position": 24, "column_name": "("},
                "items_token": {"begin_position": 24, "items": ["order_id", "user_id", "status", "c_date", "item_id"]},
                "insert_column_token": {"begin_position": 24, "column_name": ")"},
                "insert_values_token": {"begin_position": 31, "table_name": "t_order_item"}
            },
        "or_condition": [
            [
                {"table_name": "t_order_item", "column_name": "order_id", "operator": "EQUAL", "values": [(0, 1000)]},
                {"table_name": "t_order_item", "column_name": "user_id", "operator": "EQUAL", "values": [(1, 10)]},
            ]
        ]
    },
}
