from shardingpy.optimizer.insert_optimizer import InsertOptimizeEngine
from shardingpy.optimizer.query_optimizer import QueryOptimizeEngine
from shardingpy.parsing.parser.sql.dml.insert import InsertStatement


def get_optimizer(sharding_rule, sql_statement, parameters, generated_key):
    if isinstance(sql_statement, InsertStatement):
        return InsertOptimizeEngine(sharding_rule, sql_statement, parameters, generated_key)
    return QueryOptimizeEngine(sql_statement.conditions.or_condition, parameters)
