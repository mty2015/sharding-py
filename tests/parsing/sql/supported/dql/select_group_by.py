from shardingpy.constant import DatabaseType

CASES = {
    "assertSelectDateFuncWithGroupBy":
        (
            "SELECT date_format(c_date, '%%y-%%m-%%d') as c_date, count(*) as c_number FROM `t_order_item` WHERE order_id in (%s, %s) GROUP BY date_format(c_date, '%%y-%%m-%%d')",
            [DatabaseType.MySQL]),
    "assertSelectSumWithGroupBy":
        ("SELECT SUM(order_id) AS orders_sum, user_id FROM t_order GROUP BY user_id ORDER BY user_id", []),
    "assertSelectCountWithGroupBy":
        ("SELECT COUNT(order_id) AS orders_count, user_id FROM t_order GROUP BY user_id ORDER BY user_id", []),
    "assertSelectMaxWithGroupBy":
        ("SELECT MAX(order_id) AS max_order_id, user_id FROM t_order GROUP BY user_id ORDER BY user_id", []),
    "assertSelectMinWithGroupBy":
        ("SELECT MIN(order_id) AS min_order_id, user_id FROM t_order GROUP BY user_id ORDER BY user_id", []),
    "assertSelectAvgWithGroupBy":
        ("SELECT AVG(order_id) AS orders_avg, user_id FROM t_order GROUP BY user_id ORDER BY user_id", []),
    "assertSelectOrderByDescWithGroupBy":
        ("SELECT SUM(order_id) AS orders_sum, user_id FROM t_order GROUP BY user_id ORDER BY orders_sum DESC", []),
    "assertSelectCountWithoutGroupedColumn":
        (
            "SELECT count(*) as items_count FROM t_order o JOIN t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id WHERE o.user_id IN (%s, %s) AND o.order_id BETWEEN %s AND %s GROUP BY o.user_id",
            []),
    "assertSelectCountWithGroupByBindingTable":
        (
            "SELECT count(*) as items_count, o.user_id FROM t_order o JOIN t_order_item i ON o.user_id = i.user_id AND o.order_id = i.order_id WHERE o.user_id IN (%s, %s) AND o.order_id BETWEEN %s AND %s GROUP BY o.user_id ORDER BY o.user_id",
            []),
    "assertSelectWithGroupByAndLimit":
        ("SELECT user_id FROM t_order GROUP BY user_id ORDER BY user_id LIMIT %s", [DatabaseType.MySQL]),
    "assertSelectWithGroupByAndOrderByAndLimit":
        ("SELECT user_id, SUM(order_id) FROM t_order GROUP BY user_id ORDER BY SUM(order_id) LIMIT %s", []),
    "assertSelectItemWithAliasAndMatchOrderByAndGroupByItems":
        ("SELECT o.user_id uid FROM t_order o GROUP BY o.user_id ORDER BY o.user_id", []),
    "assertSelectGroupByWithAliasIsKeyword":
        ("SELECT SUM(order_id) AS orders_sum, user_id as `key` FROM t_order GROUP BY `key`", [])
}
