import logging

_LOG = logging.getLogger(__name__)


def log_sql(logic_sql, sql_statement, sql_execution_units):
    _LOG.info('Logic SQL: {}'.format(logic_sql))
    _LOG.info('SQLStatement: {}'.format(sql_statement))
    for each in sql_execution_units:
        if each.sql_unit.parameter_sets[0]:
            _LOG.info('Actual SQL: {} ::: {} ::: {}'.format(each.data_source, each.sql_unit.sql,
                                                            each.sql_unit.parameter_sets))
        else:
            _LOG.info('Actual SQL: {} ::: {}'.format(each.data_source, each.sql_unit.sql))
