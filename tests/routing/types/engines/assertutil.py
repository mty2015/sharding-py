from shardingpy.routing.types.base import RoutingResult


def assert_routing_result(test_case, routing_result, table_units_size, data):
    test_case.assertTrue(isinstance(routing_result, RoutingResult))
    test_case.assertEqual(len(routing_result.table_units.table_units), table_units_size)
    for i in range(table_units_size):
        data_source_name, routing_tables_size, routing_tables_data = data[i]
        test_case.assertTrue(assert_has_data_source_name(data_source_name, routing_result),
                             'data source name not exist: {}'.format(data_source_name))
        test_case.assertTrue(
            assert_same_table_unit(data_source_name, routing_tables_data, routing_tables_size, routing_result),
            'routing tables not match, data source name: {}, routing tables data: {}'.format(data_source_name,
                                                                                             routing_tables_data))


def assert_has_data_source_name(data_source_name, routing_result):
    for each in routing_result.table_units.table_units:
        if each.data_source_name == data_source_name:
            return True
    return False


def assert_same_table_unit(data_source_name, routing_tables_data, routing_tables_size, routing_result):
    for each in routing_tables_data:
        logic_table_name, actual_table_name = each
        for table_unit in routing_result.table_units.table_units:
            if table_unit.data_source_name == data_source_name and len(table_unit.routing_tables) == routing_tables_size:
                routing_table = table_unit.find_routing_table(data_source_name, actual_table_name)
                if routing_table and routing_table.logic_table_name == logic_table_name:
                    return True
        return False
