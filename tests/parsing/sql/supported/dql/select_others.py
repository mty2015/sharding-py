from shardingpy.constant import DatabaseType

CASES = {
    "assertSelectInWithNullParameter":
        ("SELECT * FROM t_order WHERE sku_num IN (%s, %s) ORDER BY order_id ASC", [DatabaseType.MySQL]),
    "assertSelectPaginationWithRowCount":
        (
            "SELECT i.* FROM t_order o JOIN t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id WHERE o.user_id IN (%s, %s) AND o.order_id BETWEEN %s AND %s ORDER BY i.item_id DESC LIMIT %s",
            [])
}
