from shardingpy.constant import DatabaseType
from shardingpy.constant import SQLType
from shardingpy.exception import UnsupportedOperationException
from shardingpy.parsing.parser.dialect.mysql import MySQLSelectParser
from shardingpy.parsing.parser.sql import SQLStatement
from shardingpy.parsing.token import DefaultKeyword, Symbol, Assit
from shardingpy.util import strutil, sqlutil


def new_select_parser(db_type, sharding_rule, lexer_engine):
    if db_type == DatabaseType.MySQL:
        return MySQLSelectParser(sharding_rule, lexer_engine)
    else:
        raise UnsupportedOperationException("Cannot support database {}".format(db_type))


class SelectStatement(SQLStatement):
    def __init__(self):
        super().__init__(SQLType.DQL)
        self.contain_star = False
        self.select_list_last_position = 0
        self.group_by_last_position = 0
        self.select_items = set()
        self.group_by_items = set()
        self.order_by_items = set()
        self.limit = None
        self.sub_query_statement = None

    def get_alias(self, name):
        if self.contain_star:
            return
        raw_name = sqlutil.get_exactly_value(name)
        for each in self.select_items:
            if strutil.equals_ignore_case(raw_name, sqlutil.get_exactly_value(each.expression)):
                return each.alias
            if strutil.equals_ignore_case(raw_name, each.alias):
                return raw_name

    def set_sub_query_statement(self, sub_query_statement):
        self.sub_query_statement = sub_query_statement
        self.parameters_index = sub_query_statement.parameters_index


class AbstractSelectParser:
    DERIVED_COUNT_ALIAS = "AVG_DERIVED_COUNT_%s"
    DERIVED_SUM_ALIAS = "AVG_DERIVED_SUM_%s"
    ORDER_BY_DERIVED_ALIAS = "ORDER_BY_DERIVED_%s"
    GROUP_BY_DERIVED_ALIAS = "GROUP_BY_DERIVED_%s"

    def __init__(self, sharding_rule, lexer_engine, select_clause_parser_facade):
        self.sharding_rule = sharding_rule
        self.lexer_engine = lexer_engine
        self.select_clause_parser_facade = select_clause_parser_facade
        # SelectItem
        self.select_items = list()

    def parse(self):
        """
        return: SelectStatement
        """
        result = self._parse_interal()
        if result.contains_sub_query():
            result = result.merge_sub_query_statement()

        self._append_derived_columns(result)
        self._append_derived_order_by(result)
        return result

    def _parse_interal(self):
        result = SelectStatement()
        self.lexer_engine.next_token()
        self.parse_interal(result)
        return result

    def parse_internal(self, select_statement):
        raise Exception("unimplemented")

    def parse_distinct(self):
        self.select_clause_parser_facade.distinct_clause_parser.parse()

    def parse_select_list(self, select_statement, select_items):
        self.select_clause_parser_facade.select_list_clause_parser.parse(select_statement, select_items)

    def parse_from(self, select_statement):
        self.lexer_engine.unsupported_if_equal(DefaultKeyword.INTO)
        if self.lexer_engine.skip_if_equal(DefaultKeyword.FROM):
            self._parse_table(select_statement)

    def _parse_table(self, select_statement):
        if self.lexer_engine.skip_if_equal(Symbol.LEFT_PAREN):
            select_statement.set_sub_query_statement(self._parse_interal())
            if self.lexer_engine.skip_if_equal(DefaultKeyword.WHERE, Assit.END):
                return
        self.select_clause_parser_facade.table_references_clause_parser.parse(select_statement, False)

    def parse_where(self, sharding_rule, select_statement, select_items):
        self.select_clause_parser_facade.where_clause_parser.parse(sharding_rule, select_statement, select_items)
