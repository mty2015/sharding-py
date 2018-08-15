from shardingpy.routing.base import SQLRouteResult
from shardingpy.rule.base import MasterSlaveRule


class ShardingMasterSlaveRouter:
    def __init__(self, master_slave_rules):
        self.master_slave_rules = master_slave_rules

    def route(self, sql_route_result):
        assert isinstance(sql_route_result, SQLRouteResult)
        for each in self.master_slave_rules:
            assert isinstance(each, MasterSlaveRule)
            self._route(each, sql_route_result)
        return sql_route_result

    def _route(self, master_slave_rule, sql_route_result):
        pass
