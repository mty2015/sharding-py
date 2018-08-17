import threading

from shardingpy.constant import SQLType
from shardingpy.routing.base import SQLRouteResult, SQLExecutionUnit
from shardingpy.rule.base import MasterSlaveRule
from shardingpy.util import strutil


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
        to_be_removed = list()
        to_be_added = list()
        for each in sql_route_result.execution_units:
            if not strutil.equals_ignore_case(master_slave_rule.name, each.data_source):
                continue
            to_be_removed.append(each)
            if self._is_master_route(sql_route_result.sql_statement.sql_type):
                MasterVisitedManager.set_master_visited()
                to_be_added.append(SQLExecutionUnit(master_slave_rule.master_data_source_name, each.sql_unit))
            else:
                to_be_removed.append(SQLExecutionUnit(
                    master_slave_rule.load_balance_algorithm.get_data_source(master_slave_rule.name,
                                                                             master_slave_rule.master_data_source_name,
                                                                             master_slave_rule.slave_data_source_names),
                    each.sql_unit))
        sql_route_result.execution_units = [i for i in sql_route_result.execution_units if i in to_be_removed]
        sql_route_result.execution_units.extend(to_be_added)

    def _is_master_route(self, sql_type):
        # TODO HintManager
        return sql_type != SQLType.DDL or MasterVisitedManager.is_master_visited()


class MasterVisitedManager:
    MASTER_VISITED = threading.local()
    MASTER_VISITED.flag = False

    @classmethod
    def is_master_visited(cls):
        return cls.MASTER_VISITED.flag

    @classmethod
    def set_master_visited(cls):
        cls.MASTER_VISITED.flag = True

    @classmethod
    def clear(cls):
        cls.MASTER_VISITED.flag = False
