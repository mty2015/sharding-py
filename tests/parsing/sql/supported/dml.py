from shardingpy.constant import DatabaseType

CASES = {
    "assertInsertWithAllPlaceholders":
        ("INSERT INTO t_order (order_id, user_id, status) VALUES (%s, %s, %s)", []),
    "assertInsertWithPartialPlaceholder":
        ("INSERT INTO t_order (order_id, user_id, status) VALUES (%s, %s, 'insert')", []),
    "assertInsertWithGenerateKeyColumn":
        ("INSERT INTO t_order_item(item_id, order_id, user_id, status) values (%s, %s, %s, 'insert')", []),
    "assertInsertWithoutGenerateKeyColumn":
        ("INSERT INTO t_order_item(order_id, user_id, status) values (%s, %s, 'insert')", []),
    "assertInsertOnDuplicateKeyUpdate":
        (
            "INSERT INTO t_order (order_id, user_id, status) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE status = VALUES(status)",
            [DatabaseType.MySQL]),
    "assertBatchInsertWithGenerateKeyColumn":
        (
            "INSERT INTO t_order_item(item_id, order_id, user_id, status) values (%s, %s, %s, 'insert'), (%s, %s, %s, 'insert')",
            []),
    "assertBatchInsertWithoutGenerateKeyColumn":
        ("INSERT INTO t_order_item(order_id, user_id, status) values (%s, %s, 'insert'), (%s, %s, 'insert')", []),
    "assertInsertWithJsonAndGeo":
        (
            "INSERT INTO t_place(user_new_id, guid, start_point,rule) VALUES (%s, %s, ST_GeographyFromText('SRID=4326;POINT('||%s||' '||%s||')'), %s::jsonb)",
            []),
    "assertInsertWithoutColumnsWithAllPlaceholders":
        ("INSERT INTO t_order VALUES (%s, %s, %s)", []),
    "assertInsertWithoutColumnsWithPartialPlaceholder":
        ("INSERT INTO t_order VALUES (%s, %s, 'insert')", []),
    "assertInsertWithoutColumnsWithGenerateKeyColumn":
        ("INSERT INTO t_order_item values(%s, %s, %s, 'insert', '2020-09-09')", []),
    "assertInsertWithoutColumnsWithoutGenerateKeyColumn":
        ("INSERT INTO t_order_item values(%s, %s, 'insert', '2020-09-09')", []),
    "assertUpdateWithAlias":
        ("UPDATE t_order AS o SET o.status = %s WHERE o.order_id = %s AND o.user_id = %s", [DatabaseType.MySQL]),
    "assertUpdateWithoutAlias":
        ("UPDATE t_order SET status = %s WHERE order_id = %s AND user_id = %s", []),
    "assertDeleteWithShardingValue":
        ("DELETE FROM t_order WHERE order_id = %s AND user_id = %s AND status=%s", []),
    "assertDeleteWithoutShardingValue":
        ("DELETE FROM t_order WHERE status=%s", [])
}
