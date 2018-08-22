import unittest

from shardingpy.api.config.base import load_sharding_rule_config_from_dict
from shardingpy.routing.types.base import RoutingResult, RoutingTable
from shardingpy.routing.types.engines.unicast import UnicastRoutingEngine
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


class UnicastRoutingEngineTest(unittest.TestCase):
    def setUp(self):
        sharding_rule_config = load_sharding_rule_config_from_dict(rule_config['sharding_rule'])
        self.sharding_rule = ShardingRule(sharding_rule_config, ['ds0', 'ds1'])

    def test_empty_tables(self):
        routing_result = UnicastRoutingEngine(self.sharding_rule, []).route()
        self.assertTrue(isinstance(routing_result, RoutingResult))
        self.assertEqual(len(routing_result.table_units.table_units), 1)
        self.assertEqual(routing_result.table_units.table_units[0].data_source_name, 'ds0')
        self.assertFalse(routing_result.table_units.table_units[0].routing_tables)

    def test_empty_tables(self):
        routing_result = UnicastRoutingEngine(self.sharding_rule, []).route()
        self.assertTrue(isinstance(routing_result, RoutingResult))
        self.assertEqual(len(routing_result.table_units.table_units), 1)
        self.assertEqual(routing_result.table_units.table_units[0].data_source_name, 'ds0')
        self.assertFalse(routing_result.table_units.table_units[0].routing_tables)

    def test_single_table(self):
        routing_result = UnicastRoutingEngine(self.sharding_rule, ['t_order']).route()
        self.assertTrue(isinstance(routing_result, RoutingResult))
        self.assertEqual(len(routing_result.table_units.table_units), 1)
        self.assertEqual(routing_result.table_units.table_units[0].data_source_name, 'ds0')
        self.assertEqual(len(routing_result.table_units.table_units[0].routing_tables), 1)
        self.assertEqual(routing_result.table_units.table_units[0].routing_tables[0],
                         RoutingTable('t_order', 't_order_0'))
