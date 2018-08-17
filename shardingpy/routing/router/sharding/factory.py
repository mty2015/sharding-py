from shardingpy.routing.router.sharding.impl import ParsingSQLRouter


def create_sql_router(sharding_rule, sharding_meta_data, database_type, show_sql):
    # TODO HintManagerHolder
    return ParsingSQLRouter(sharding_rule, sharding_meta_data, database_type, show_sql)

