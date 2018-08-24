import unittest

from shardingpy.api.config.base import load_sharding_rule_config_from_dict
from shardingpy.routing.types.base import RoutingResult, RoutingTable, TableUnit
from shardingpy.routing.types.engines.complex import CartesianRoutingEngine
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


class CartesianRoutingEngineTest(unittest.TestCase):
    def setUp(self):
        sharding_rule_config = load_sharding_rule_config_from_dict(rule_config['sharding_rule'])
        self.sharding_rule = ShardingRule(sharding_rule_config, ['ds0', 'ds1'])

    def test_empty_tables(self):
        routing_results = list()

        routing_result = RoutingResult()
        table_unit = TableUnit('ds0')
        table_unit.routing_tables.extend([RoutingTable('t_user', 't_user_0')])
        routing_result.table_units.table_units.append(table_unit)
        table_unit = TableUnit('ds0')
        table_unit.routing_tables.extend([RoutingTable('t_user', 't_user_1')])
        routing_result.table_units.table_units.append(table_unit)
        table_unit = TableUnit('ds1')
        table_unit.routing_tables.extend([RoutingTable('t_user', 't_user_0')])
        routing_result.table_units.table_units.append(table_unit)
        table_unit = TableUnit('ds1')
        table_unit.routing_tables.extend([RoutingTable('t_user', 't_user_1')])
        routing_result.table_units.table_units.append(table_unit)
        routing_results.append(routing_result)

        routing_result = RoutingResult()
        table_unit = TableUnit('ds0')
        table_unit.routing_tables.extend([RoutingTable('t_order', 't_order_0')])
        routing_result.table_units.table_units.append(table_unit)
        table_unit = TableUnit('ds0')
        table_unit.routing_tables.extend([RoutingTable('t_order', 't_order_1')])
        routing_result.table_units.table_units.append(table_unit)
        table_unit = TableUnit('ds1')
        table_unit.routing_tables.extend([RoutingTable('t_order', 't_order_0')])
        routing_result.table_units.table_units.append(table_unit)
        table_unit = TableUnit('ds1')
        table_unit.routing_tables.extend([RoutingTable('t_order', 't_order_1')])
        routing_result.table_units.table_units.append(table_unit)
        routing_results.append(routing_result)

        routing_result = CartesianRoutingEngine(routing_results).route()
        assert_routing_result(self, routing_result, 8,
                              [('ds0', 2, [('t_user', 't_user_0'), ('t_order', 't_order_0')]),
                               ('ds0', 2, [('t_user', 't_user_0'), ('t_order', 't_order_1')]),
                               ('ds0', 2, [('t_user', 't_user_1'), ('t_order', 't_order_0')]),
                               ('ds0', 2, [('t_user', 't_user_1'), ('t_order', 't_order_1')]),
                               ('ds1', 2, [('t_user', 't_user_0'), ('t_order', 't_order_0')]),
                               ('ds1', 2, [('t_user', 't_user_0'), ('t_order', 't_order_1')]),
                               ('ds1', 2, [('t_user', 't_user_1'), ('t_order', 't_order_0')]),
                               ('ds1', 2, [('t_user', 't_user_1'), ('t_order', 't_order_1')])])
