from shardingpy.constant import DatabaseType

CASES = {
    "assertSelectExpressionWithSingleTable": ("SELECT o.order_id + 1 * 2 FROM t_order AS o ORDER BY o.order_id", []),
    "assertSelectDateFuncWithSingleTable":
        ("SELECT DATE(i.c_date) AS c_date FROM `t_order_item` AS i ORDER BY DATE(i.c_date) DESC", [DatabaseType.MySQL]),
    "assertSelectCountWithExpression":
        ("SELECT COUNT(o.order_id) + 1^2 FROM t_order o", []),
    "assertSelectRegexpWithSingleTable":
        ("SELECT * FROM t_order_item t WHERE t.status REGEXP %s AND t.item_id IN (%s, %s)", [])
}
