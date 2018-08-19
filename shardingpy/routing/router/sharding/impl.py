from shardingpy.optimizer.factory import get_optimizer
from shardingpy.parsing.parser.parser_engine import SQLParsingEngine
from shardingpy.parsing.parser.sql.dml.insert import InsertStatement
from shardingpy.parsing.parser.sql.dql.select import SelectStatement
from shardingpy.rewrite.rewrite_engine import SQLRewriteEngine
from shardingpy.routing.base import SQLRouteResult, SQLExecutionUnit
from shardingpy.routing.router.sharding.base import ShardingRouter, GeneratedKey
from shardingpy.routing.types.base import RoutingResult
from shardingpy.util import sql_logger


class ParsingSQLRouter(ShardingRouter):
    def __init__(self, sharding_rule, sharding_meta_data, database_type, show_sql):
        self.sharding_rule = sharding_rule
        self.sharding_meta_data = sharding_meta_data
        self.database_type = database_type
        self.show_sql = show_sql
        self.generated_keys = list()

    def parse(self, logic_sql, user_cache):
        return SQLParsingEngine(self.database_type, logic_sql, self.sharding_rule, self.sharding_meta_data).parse()

    def route(self, logic_sql, parameters, sql_statement):
        generated_key = None
        if isinstance(sql_statement, InsertStatement):
            generated_key = self._get_generate_key(sql_statement, parameters)
        result = SQLRouteResult(sql_statement, generated_key)
        sharding_conditions = get_optimizer(self.sharding_rule, sql_statement, parameters, generated_key).optimize()
        if not generated_key:
            self._set_generated_keys(result, generated_key)

        routing_result = self._route(parameters, sql_statement, sharding_conditions)
        rewrite_engine = SQLRewriteEngine(self.sharding_rule, logic_sql, self.database_type, sql_statement,
                                          sharding_conditions, parameters)
        is_single_routing = routing_result.is_single_routing()
        if isinstance(sql_statement, SelectStatement) and sql_statement.limit:
            self._process_limit(parameters, sql_statement, is_single_routing)
        sql_builder = rewrite_engine.rewrite(not is_single_routing)
        for each in routing_result.table_units.table_units:
            result.execution_units.append(
                SQLExecutionUnit(each.data_source_name, rewrite_engine.generate_sql(each, sql_builder)))
        if self.show_sql:
            sql_logger.log_sql(logic_sql, sql_statement, result.execution_units)
        return result

    def _route(self, parameters, sql_statement, sharding_conditions):
        return RoutingResult()

    def _get_generate_key(self, insert_statement, parameters):
        generated_key = None
        if insert_statement.generate_key_column_index != -1:
            for each in insert_statement.generated_key_conditions:
                if generated_key is None:
                    generated_key = GeneratedKey(each.column)
                if each.index == -1:
                    generated_key.generated_keys.append(each.value)
                else:
                    generated_key.generated_keys.append(parameters[each.index])
            return generated_key

        logic_table_name = insert_statement.tables.get_single_table_name()
        table_rule = self.sharding_rule.try_find_table_rule_by_logic_table(logic_table_name)
        if not table_rule:
            return None
        generate_key_column = self.sharding_rule.get_generate_key_column(logic_table_name)
        if generate_key_column:
            generated_key = GeneratedKey(generate_key_column)
            for i in range(len(insert_statement.insert_values.insert_values)):
                generated_key.generated_keys.append(self.sharding_rule.generate_key(logic_table_name))

        return generated_key

    def _set_generated_keys(self, sql_route_result, generated_key):
        self.generated_keys.extend(generated_key.generated_keys)
        sql_route_result.generated_key.generated_keys.clear()
        sql_route_result.generated_key.generated_keys.extend(self.generated_keys)

    def _process_limit(self, parameters, sql_statement, is_single_routing):
        if is_single_routing:
            sql_statement.limit = None
            return

        is_need_fetch_all = (sql_statement.group_by_items or sql_statement.get_aggregation_select_items()) and \
                            not sql_statement.is_same_group_by_and_order_by_items()
        sql_statement.limit.process_parameters(parameters, is_need_fetch_all)
