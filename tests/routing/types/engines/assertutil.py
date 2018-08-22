from shardingpy.routing.types.base import RoutingResult


def assert_routing_result(test_case, routing_result, table_units_size, data):
    test_case.assertTrue(isinstance(routing_result, RoutingResult))
    test_case.assertEqual(len(routing_result.table_units.table_units), table_units_size)
    for i in range(table_units_size):
        data_source_name, routing_tables_size, routing_tables_data = data[i]
        table_unit = routing_result.table_units.table_units[i]
        test_case.assertEqual(table_unit.data_source_name, data_source_name)
        test_case.assertEqual(len(table_unit.routing_tables), routing_tables_size)
        for j in range(len(routing_tables_data)):
            logic_table_name, actual_table_name = routing_tables_data[j]
            routing_table = table_unit.routing_tables[j]
            test_case.assertEqual(routing_table.logic_table_name, logic_table_name)
            test_case.assertEqual(routing_table.actual_table_name, actual_table_name)
