from shardingpy.routing.router.masterslave.base import ShardingMasterSlaveRouter
from shardingpy.routing.router.sharding.factory import create_sql_router


class StatementRoutingEngine:
    def __init__(self, sharding_rule, sharding_meta_data, database_type, show_sql):
        self.sharding_router = create_sql_router(sharding_rule, sharding_meta_data, database_type, show_sql)
        self.master_slave_router = ShardingMasterSlaveRouter(sharding_rule.master_slave_rules)

    def route(self, logic_sql):
        """
        :param logic_sql:
        :return: SQLRouteResult
        """
        sql_statement = self.sharding_router.parse(logic_sql, False)
        return self.master_slave_router.route(self.sharding_router.route(logic_sql, list(), sql_statement))


class PreparedStatementRoutingEngine:
    def __init__(self, logic_sql, sharding_rule, sharding_meta_data, database_type, show_sql):
        self.logic_sql = logic_sql
        self.sharding_router = create_sql_router(sharding_rule, sharding_meta_data, database_type, show_sql)
        self.master_slave_router = ShardingMasterSlaveRouter(sharding_rule.master_slave_rules)
        self.sql_statement = None

    def route(self, parameters):
        if self.sql_statement is None:
            self.sql_statement = self.sharding_router.parse(self.logic_sql, True)
        return self.master_slave_router.route(
            self.sharding_router.route(self.logic_sql, parameters, self.sql_statement))
