from shardingpy.constant import DatabaseType

CASES = {"assertSelectOne":
             ("SELECT 1 as a",
              []),
         "assertSelectNotEqualsWithSingleTable":
             ("SELECT * FROM t_order_item WHERE item_id <> %s ORDER BY item_id",
              [])
         }
