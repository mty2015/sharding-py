import unittest

from shardingpy.api.config.base import load_sharding_rule_config_from_dict
from shardingpy.routing.types.base import RoutingResult
from shardingpy.routing.types.engines.broadcast import DatabaseBroadcastRoutingEngine
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
                                      'ds1.t_order_1', 'ds1.t_order_2']
            }
        }
    }
}


class DatabaseBroadcastRoutingEngineTest(unittest.TestCase):

    def setUp(self):
        sharding_rule_config = load_sharding_rule_config_from_dict(rule_config['sharding_rule'])
        sharding_rule = ShardingRule(sharding_rule_config, ['ds0', 'ds1'])
        self.database_broadcast_routing_engine = DatabaseBroadcastRoutingEngine(sharding_rule)

    def test_route(self):
        routing_result = self.database_broadcast_routing_engine.route()
        self.assertTrue(isinstance(routing_result, RoutingResult))
        self.assertEqual(len(routing_result.table_units.table_units), 2)
        self.assertEqual(routing_result.table_units.table_units[0].data_source_name, 'ds0')
        self.assertEqual(routing_result.table_units.table_units[1].data_source_name, 'ds1')
