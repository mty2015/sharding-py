import unittest

from shardingpy.api.config.base import load_sharding_rule_config_from_dict
from shardingpy.constant import SQLType
from shardingpy.parsing.parser.sql import SQLStatement
from shardingpy.parsing.parser.sql.dql.select import SelectStatement
from shardingpy.parsing.parser.token import IndexToken
from shardingpy.routing.types.base import RoutingResult
from shardingpy.routing.types.engines.broadcast import TableBroadcastRoutingEngine
from shardingpy.rule.base import ShardingRule

rule_config = {
    'data_sources': {
        'ds0': None,
        'ds1': None
    },
    'sharding_rule': {
        'tables': {
            't_order': {
                'actual_data_nodes': ['ds0.t_order_0', 'ds0.t_order_1', 'ds0.t_order_2', 'ds1.t_order_0',
                                      'ds1.t_order_1', 'ds1.t_order_2'],
                'logic_index': 't_order_index'
            }
        }
    }
}


class TableBroadcastRoutingEngineTest(unittest.TestCase):

    def setUp(self):
        sharding_rule_config = load_sharding_rule_config_from_dict(rule_config['sharding_rule'])
        self.sharding_rule = ShardingRule(sharding_rule_config, ['ds0', 'ds1'])

    def test_route_dql_statement(self):
        routing_result = self._create_dql_statement_routing_result()
        self.assertTrue(isinstance(routing_result, RoutingResult))
        self.assertFalse(routing_result.is_single_routing())
        self.assertEqual(len(routing_result.table_units.table_units), 0)

    def test_route_ddl_statement(self):
        routing_result = self._create_ddl_statement_routing_result()
        self.assertTrue(isinstance(routing_result, RoutingResult))
        self.assertFalse(routing_result.is_single_routing())
        self.assertEqual(len(routing_result.table_units.table_units), 6)

    def _create_dql_statement_routing_result(self):
        routing_engine = TableBroadcastRoutingEngine(self.sharding_rule, SelectStatement())
        return routing_engine.route()

    def _create_ddl_statement_routing_result(self):
        ddl_statement = SQLStatement(SQLType.DDL)
        ddl_statement.sql_tokens.append(IndexToken(13, 't_order', 't_order_index'))
        routing_engine = TableBroadcastRoutingEngine(self.sharding_rule, ddl_statement)
        return routing_engine.route()
