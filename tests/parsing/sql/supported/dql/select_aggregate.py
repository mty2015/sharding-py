CASES = {
    "assertSelectSum": ("SELECT SUM(user_id) AS user_id_sum FROM t_order", []),
    "assertSelectCount": ("SELECT COUNT(*) AS orders_count FROM t_order", []),
    "assertSelectMax": ("SELECT max(user_id) AS max_user_id FROM t_order", []),
    "assertSelectMin": ("SELECT MIN(user_id) AS min_user_id FROM t_order", []),
    "assertSelectAvg": ("SELECT AVG(user_id) AS user_id_avg FROM t_order", []),
    "assertSelectCountForSpecialSymbol": ("SELECT COUNT(`order_id`) AS orders_count FROM t_order", [])
}
