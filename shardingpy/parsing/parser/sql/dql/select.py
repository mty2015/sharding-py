from shardingpy.constant import DatabaseType
from shardingpy.constant import SQLType
from shardingpy.exception import UnsupportedOperationException
from shardingpy.parsing.parser.dialect.mysql import MySQLSelectParser
from shardingpy.parsing.parser.sql import SQLStatement
from shardingpy.parsing.token import DefaultKeyword, Symbol, Assit
from shardingpy.parsing.parser.context.selectitem import AggregationSelectItem
from shardingpy.parsing.parser.token import ItemsToken, OrderByToken
from shardingpy.constant import AggregationType
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
        self.group_by_items = list()
        self.order_by_items = list()
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

    def contains_sub_query(self):
        return self.sub_query_statement is not None

    def set_sub_query_statement(self, sub_query_statement):
        self.sub_query_statement = sub_query_statement
        self.parameters_index = sub_query_statement.parameters_index

    def merge_sub_query_statement(self):
        raise Exception("TODO")


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
        result = self._parse_internal()
        if result.contains_sub_query():
            result = result.merge_sub_query_statement()

        self._append_derived_columns(result)
        self._append_derived_order_by(result)
        return result

    def _parse_internal(self):
        result = SelectStatement()
        self.lexer_engine.next_token()
        self.parse_internal(result)
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

    def parse_group_by(self, select_statement):
        self.select_clause_parser_facade.group_by_clause_parser.parse(select_statement)

    def parse_having(self):
        self.select_clause_parser_facade.having_clause_parser.parse()

    def parse_order_by(self, select_statement):
        self.select_clause_parser_facade.order_by_clause_parser.parse(select_statement)

    def parse_select_rest(self):
        self.select_clause_parser_facade.select_rest_clause_parser.parse()

    def _append_derived_columns(self, select_statement):
        items_token = ItemsToken(select_statement.select_list_last_position)
        self._append_avg_derived_columns(items_token, select_statement)
        self._append_derived_order_columns(items_token, select_statement.order_by_items, self.ORDER_BY_DERIVED_ALIAS,
                                           select_statement)
        self._append_derived_order_columns(items_token, select_statement.group_by_items, self.ORDER_BY_DERIVED_ALIAS,
                                           select_statement)
        if items_token.items:
            select_statement.sql_tokens.append(items_token)

    def _append_avg_derived_columns(self, items_token, select_statement):
        derived_column_offset = 0
        for each in select_statement.select_items:
            if not (isinstance(each, AggregationSelectItem) and each.aggregation_type == AggregationType.AVG):
                continue
            count_alias = self.DERIVED_COUNT_ALIAS % derived_column_offset
            count_item = AggregationSelectItem(AggregationType.COUNT, each.inner_expression, count_alias)
            sum_alias = self.DERIVED_SUM_ALIAS % derived_column_offset
            sum_item = AggregationSelectItem(AggregationType.SUM, each.inner_expression, sum_alias)
            each.derived_aggregation_select_items.extend([count_item, sum_item])
            items_token.items.append(count_item.expression + " AS " + count_alias + " ")
            items_token.items.append(sum_item.expression + " AS " + sum_alias + " ")
            derived_column_offset += 1

    def _append_derived_order_columns(self, items_token, order_items, alias_pattern, select_statement):
        derived_column_offset = 0
        for each in order_items:
            if not self._is_contains_item(each, select_statement):
                alias = alias_pattern % derived_column_offset
                each.alias = alias
                items_token.items.append(each.get_qualified_name() + " AS " + alias)

    def _is_contains_item(self, order_item, select_statement):
        if select_statement.contain_star():
            return True
        for each in select_statement.select_items:
            if each.index != -1:
                # ORDER BY [position]
                return True
            if each.alias and order_item.alias and strutil.equals_ignore_case(each.alias, order_item.alias):
                return True
            if not each.alias and order_item.get_qualified_name() and strutil.equals_ignore_case(each.expression,
                                                                                                 order_item.get_qualified_name()):
                return True
        return False

    def _append_derived_order_by(self, select_statement):
        if select_statement.group_by_items and not select_statement.order_by_items:
            select_statement.order_by_items.extend(select_statement.group_by_items)
            select_statement.sql_tokens.append(OrderByToken(select_statement.select_list_last_position))
