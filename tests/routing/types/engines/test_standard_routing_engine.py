import unittest

from shardingpy.api.config.base import load_sharding_rule_config_from_dict
from shardingpy.optimizer.condition import ShardingConditions
from shardingpy.routing.types.base import RoutingResult, RoutingTable
from shardingpy.routing.types.engines.standard import StandardRoutingEngine
from shardingpy.routing.types.engines.unicast import UnicastRoutingEngine
from shardingpy.rule.base import ShardingRule
from tests.routing.types.engines.assertutil import assert_routing_result

rule_config = {
    'data_sources': {
        'ds0': None,
        'ds1': None
    },
    'sharding_rule': {
        'tables': {
            't_order': {
                'actual_data_nodes': ['ds0.t_order_0', 'ds0.t_order_1', 'ds1.t_order_0', 'ds1.t_order_1']
            },
            't_order_item': {
                'actual_data_nodes': ['ds0.t_order_item_0', 'ds0.t_order_item_1', 'ds1.t_order_item_0',
                                      'ds1.t_order_item_1']
            }
        },
        'binding_tables': [('t_order', 't_order_item')]
    }
}


class StandardRoutingEngineTest(unittest.TestCase):
    def setUp(self):
        sharding_rule_config = load_sharding_rule_config_from_dict(rule_config['sharding_rule'])
        self.sharding_rule = ShardingRule(sharding_rule_config, ['ds0', 'ds1'])

    def test_empty_tables(self):
        routing_result = StandardRoutingEngine(self.sharding_rule, 't_order', ShardingConditions([])).route()
        assert_routing_result(self, routing_result, 4,
                              [('ds0', 1, [('t_order', 't_order_0')]), ('ds0', 1, [('t_order', 't_order_1')]),
                               ('ds1', 1, [('t_order', 't_order_0')]), ('ds1', 1, [('t_order', 't_order_1')])])
