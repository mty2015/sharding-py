from shardingpy.routing.router.sharding.factory import create_sql_router


class StatementRoutingEngine:
    def __init__(self, sharding_rule, sharding_meta_data, database_type, show_sql):
        self.sharding_router = create_sql_router(sharding_rule, sharding_meta_data, database_type, show_sql)
        self.master_slave_router = ShardingMasterSlaveRouter(sharding_rule.master_slave_rules)
