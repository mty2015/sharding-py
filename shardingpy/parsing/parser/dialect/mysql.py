from shardingpy.parsing.dialect.mysql import MySQLKeyword
from shardingpy.parsing.parser.clauseparser import DistinctClauseParser
from shardingpy.parsing.parser.clauseparser import SelectListClauseParser, TableReferencesClauseParser, \
    WhereClauseParser
from shardingpy.parsing.parser.expressionparser import AliasExpressionParser
from shardingpy.parsing.parser.sql.dql.select import AbstractSelectParser
from shardingpy.parsing.token import DefaultKeyword


class MySQLSelectClauseParserFacade:
    def __init__(self, sharding_rule, lexer_engine):
        self.distinct_clause_parser = MySQLDistinctClauseParser(lexer_engine)
        self.select_list_clause_parser = SelectListClauseParser(sharding_rule, lexer_engine)
        self.table_references_clause_parser = MySQLTableReferencesClauseParser(sharding_rule, lexer_engine)
        self.where_clause_parser = MySQLWhereClauseParser(lexer_engine)
        # self.group_by_clause_parser = MySQLGroupByClauseParser(lexer_engine)
        # self.having_clause_parser = HavingClauseParser(lexer_engine)
        # self.order_by_clause_parser = OrderByClauseParser(lexer_engine)
        # self.select_rest_clause_parser = MySQLSelectRestClauseParser(lexer_engine)


class MySQLSelectParser(AbstractSelectParser):
    def __init__(self, sharding_rule, lexer_engine):
        super().__init__(sharding_rule, lexer_engine, MySQLSelectClauseParserFacade(sharding_rule, lexer_engine))
        self.select_option_clause_parser = MySQLSelectOptionClauseParser(lexer_engine)
        # self.limit_clause_parser = MySQLLimitClauseParser(lexer_engine)

    def parse_interal(self, select_statement):
        self.parse_distinct()
        self._parse_select_option()
        self.parse_select_list(select_statement, self.select_items)
        self.parse_from(select_statement)
        self.parse_where(self.sharding_rule, select_statement, self.select_items)
        self.parse_group_by(select_statement)
        self.parse_having()
        self.parse_order_by(select_statement)
        self._parse_limit(select_statement)
        self.parse_select_reset()

    def _parse_select_option(self):
        self.select_option_clause_parser.parse()

    def _parse_limit(self, select_statement):
        self.limit_clause_parser.parse(select_statement)


class MySQLDistinctClauseParser(DistinctClauseParser):
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine

    def get_synonymous_keywords_for_distinct(self):
        return []


class MySQLSelectOptionClauseParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine

    def parse(self):
        self.lexer_engine.skip_all(MySQLKeyword.HIGH_PRIORITY, MySQLKeyword.STRAIGHT_JOIN,
                                   MySQLKeyword.SQL_SMALL_RESULT, MySQLKeyword.SQL_BIG_RESULT,
                                   MySQLKeyword.SQL_BUFFER_RESULT, MySQLKeyword.SQL_CACHE, MySQLKeyword.SQL_NO_CACHE,
                                   MySQLKeyword.SQL_CALC_FOUND_ROWS)


class MySQLTableReferencesClauseParser(TableReferencesClauseParser):
    def __init__(self, sharding_rule, lexer_engine):
        super().__init__(sharding_rule, lexer_engine)


class MySQLAliasExpressionParser(AliasExpressionParser):
    def __init__(self, lexer_engine):
        super().__init__(lexer_engine)

    def get_customized_available_keywords_for_table_alias(self):
        return [DefaultKeyword.LENGTH]


class MySQLWhereClauseParser(WhereClauseParser):
    def get_customized_other_condition_operators(self):
        return [MySQLKeyword.REGEXP]
