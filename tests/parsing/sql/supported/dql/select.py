from shardingpy.constant import DatabaseType

CASES = {
    "assertSelectOne":
        ("SELECT 1 as a",
         []),
    "assertSelectNotEqualsWithSingleTable":
        ("SELECT * FROM t_order_item WHERE item_id <> %s ORDER BY item_id",
         []),
    "assertSelectNotEqualsWithSingleTableForExclamationEqual":
        ("SELECT * FROM t_order_item WHERE item_id != %s ORDER BY item_id", []),
    "assertSelectNotEqualsWithSingleTableForNotIn":
        (
            "SELECT * FROM t_order_item WHERE item_id IS NOT NULL AND item_id NOT IN (%s, %s) ORDER BY item_id",
            []),
    "assertSelectNotEqualsWithSingleTableForNotBetween":
        (
            "SELECT * FROM t_order_item WHERE item_id IS NOT NULL AND item_id NOT BETWEEN %s AND %s ORDER BY item_id",
            []),
    "assertSelectEqualsWithSingleTable":
        ("SELECT * FROM t_order WHERE user_id = %s AND order_id = %s", []),
    "assertSelectEqualsWithSameShardingColumns":
        ("SELECT * FROM t_order WHERE order_id = %s AND order_id = %s", []),
    "assertSelectBetweenWithSingleTable":
        (
            "SELECT * FROM t_order WHERE user_id BETWEEN %s AND %s AND order_id BETWEEN %s AND %s ORDER BY user_id, order_id",
            []),
    "assertSelectInWithSingleTable":
        ("SELECT * FROM t_order WHERE user_id IN (%s, %s, %s) AND order_id IN (%s, %s) ORDER BY user_id, order_id",
         []),
    "assertSelectInWithSameShardingColumns":
        ("SELECT * FROM t_order WHERE order_id IN (%s, %s) AND order_id IN (%s, %s) ORDER BY order_id", []),
    "assertSelectIterator":
        ("SELECT t.* FROM t_order_item t WHERE t.item_id IN (%s, %s)", []),
    "assertSelectNoShardingTable":
        (
            "SELECT i.* FROM t_order o JOIN t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id ORDER BY i.item_id",
            []),
    "assertSelectLikeWithCount":
        (
            "SELECT count(0) as orders_count FROM `t_order` o WHERE o.status LIKE CONCAT('%%', %s, '%%') AND o.`user_id` IN (%s, %s) AND o.`order_id` BETWEEN %s AND %s",
            [DatabaseType.MySQL]),
    "assertSelectWithBindingTable":
        (
            "SELECT i.* FROM t_order o JOIN t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id WHERE o.user_id IN (%s, %s) AND o.order_id BETWEEN %s AND %s ORDER BY i.item_id",
            []),
    "assertSelectWithBindingTableAndConfigTable":
        (
            "SELECT i.* FROM t_order o JOIN t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id JOIN t_config c ON o.status = c.status WHERE o.user_id IN (%s, %s) AND o.order_id BETWEEN %s AND %s AND c.status = %s ORDER BY i.item_id",
            []),
    "assertSelectWithUpperCaseBindingTable":
        (
            "SELECT i.* FROM T_ORDER o JOIN T_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id WHERE o.user_id IN (%s, %s) AND o.order_id BETWEEN %s AND %s ORDER BY i.item_id",
            []),
    "assertSelectWithUpperCaseBindingTableAndConfigTable": (
        "SELECT i.*,c.status c_status FROM T_ORDER o JOIN T_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id JOIN t_config c ON o.status = c.status WHERE o.user_id IN (%s, %s) AND o.order_id BETWEEN %s AND %s AND c.status = %s ORDER BY i.item_id",
        []
    ),
    "assertSelectCountWithBindingTable":
        (
            "SELECT COUNT(*) AS items_count FROM t_order o, t_order_item i WHERE o.user_id = i.user_id AND o.order_id = i.order_id AND o.user_id IN (%s, %s) AND o.order_id BETWEEN %s AND %s",
            []),
    "assertSelectCountWithBindingTableWithJoin":
        (
            "SELECT COUNT(*) AS items_count FROM t_order o JOIN t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id WHERE o.user_id IN (%s, %s) AND o.order_id BETWEEN %s AND %s",
            []),
    "assertSelectAliasWithKeyword":
        ("SELECT length.item_id password FROM t_order_item length where length.item_id = %s ", [])
}
