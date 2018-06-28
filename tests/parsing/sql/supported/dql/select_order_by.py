from shardingpy.constant import DatabaseType

CASES = {
    "assertSelectWithOrderBy":
        ("SELECT * FROM t_order o ORDER BY o.order_id, 2 DESC", [DatabaseType.MySQL]),
    "assertSelectWithOrderByForIndex":
        (
            "SELECT i.* FROM t_order o, t_order_item i WHERE o.order_id = i.order_id AND o.status = 'init' ORDER BY o.order_id DESC, 1",
            [])
}
