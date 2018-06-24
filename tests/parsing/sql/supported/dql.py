from shardingpy.constant import DatabaseType

CASES = {"assertSelectOne":
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
             [])
         }
