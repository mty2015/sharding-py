from shardingpy.constant import AggregationType, ShardingOperator, OrderType
from shardingpy.exception import UnsupportedOperationException, SQLParsingException
from shardingpy.parsing.parser.context.condition import Column, Condition
from shardingpy.parsing.parser.context.others import OrderItem
from shardingpy.parsing.parser.context.selectitem import StarSelectItem, AggregationSelectItem, CommonSelectItem
from shardingpy.parsing.parser.context.table import Table
from shardingpy.parsing.parser.dialect import create_alias_expression_parser, create_basic_expression_parser
from shardingpy.parsing.parser.expressionparser import SQLPropertyExpression, SQLPlaceholderExpression, \
    SQLNumberExpression, SQLTextExpression, SQLIdentifierExpression, SQLIgnoreExpression
from shardingpy.parsing.parser.token import TableToken
from shardingpy.parsing.lexer.token import DefaultKeyword, Symbol
from shardingpy.util import sqlutil


class DistinctClauseParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine

    def parse(self):
        self.lexer_engine.skip_all(DefaultKeyword.ALL)
        distinct_keywords = [DefaultKeyword.DISTINCT, *self.get_synonymous_keywords_for_distinct()]
        self.lexer_engine.unsupported_if_equal(*distinct_keywords)

    def get_synonymous_keywords_for_distinct(self):
        return []


class SelectListClauseParser:
    def __init__(self, sharding_rule, lexer_engine):
        self.sharding_rule = sharding_rule
        self.lexer_engine = lexer_engine
        self.alias_expression_parser = create_alias_expression_parser(lexer_engine)

    def parse(self, select_statement, select_items):
        while True:
            select_statement.select_items.append(self._parse_select_item(select_statement))
            if not self.lexer_engine.skip_if_equal(Symbol.COMMA):
                break
        select_statement.select_list_last_position = self.lexer_engine.get_current_token().end_position - len(
            self.lexer_engine.get_current_token().literals)
        select_items.extend(list(select_statement.select_items))

    def _parse_select_item(self, select_statement):
        self.lexer_engine.skip_if_equal(*self.get_skipped_keywords_before_select_item())
        if self.is_row_number_select_item():
            result = self.parse_row_number_select_item(select_statement)
        elif self._is_star_select_item():
            select_statement.contain_star = True
            result = self._parse_star_select_item()
        elif self._is_aggregation_select_item(select_statement):
            result = self._parse_aggregation_select_item(select_statement)
            self._parse_rest_select_item(select_statement)
        else:
            result = CommonSelectItem(sqlutil.get_exactly_value(
                self._parse_common_select_item(select_statement) + self._parse_rest_select_item(select_statement)),
                self.alias_expression_parser.parse_select_item_alias())
        return result

    def get_skipped_keywords_before_select_item(self):
        return []

    def is_row_number_select_item(self):
        return False

    def parse_row_number_select_item(self, select_statement):
        raise UnsupportedOperationException("Cannot support special select item")

    def _is_star_select_item(self):
        return sqlutil.get_exactly_value(self.lexer_engine.get_current_token().literals) == Symbol.STAR.value

    def _parse_star_select_item(self):
        self.lexer_engine.next_token()
        self.alias_expression_parser.parse_select_item_alias()
        return StarSelectItem()

    def _is_aggregation_select_item(self):
        return self.lexer_engine.equal_any(DefaultKeyword.MAX, DefaultKeyword.MIN, DefaultKeyword.SUM,
                                           DefaultKeyword.AVG, DefaultKeyword.COUNT)

    def _parse_aggregation_select_item(self, select_statement):
        aggregation_type = AggregationType[self.lexer_engine.get_current_token().literals.upper()]
        self.lexer_engine.next_token()
        return AggregationSelectItem(aggregation_type, self.lexer_engine.skip_parentheses(select_statement),
                                     self.alias_expression_parser.parse_select_item_alias())

    def _parse_common_select_item(self, select_statement):
        result = ""
        literals = self.lexer_engine.get_current_token().literals
        result += literals
        position = self.lexer_engine.get_current_token().end_position - len(literals)
        self.lexer_engine.next_token()
        if self.lexer_engine.equal_any(Symbol.LEFT_PAREN):
            result += self.lexer_engine.skip_parentheses(select_statement)
        elif self.lexer_engine.equal_any(Symbol.DOT):
            table_name = sqlutil.get_exactly_value(literals)
            if self.sharding_rule.try_find_table_rule(table_name) or self.sharding_rule.find_binding_table_rule():
                select_statement.sql_tokens.append(TableToken(position, literals))
            result += self.lexer_engine.get_current_token().literals
            self.lexer_engine.next_token()
            result += self.lexer_engine.get_current_token().literals
            self.lexer_engine.next_token()
        return result

    def _parse_rest_select_item(self, select_statement):
        result = ""
        while self.lexer_engine.equal_any(*Symbol.operators()):
            result += self.lexer_engine.get_current_token().literals
            self.lexer_engine.next_token()
            result += self._parse_common_select_item(select_statement)


class TableReferencesClauseParser:
    """
    syntax: https://dev.mysql.com/doc/refman/5.7/en/join.html
    """

    def __init__(self, sharding_rule, lexer_engine):
        self.sharding_rule = sharding_rule
        self.lexer_engine = lexer_engine
        self.alias_expression_parser = create_alias_expression_parser(lexer_engine)
        self.basic_expression_parser = create_basic_expression_parser(lexer_engine)

    def parse(self, sql_statement, is_single_table_only):
        while True:
            self.parse_table_reference(sql_statement, is_single_table_only)
            if not self.lexer_engine.skip_if_equal(Symbol.COMMA):
                break

    def parse_table_reference(self, sql_statement, is_single_table_only):
        self.parse_table_factor(sql_statement, is_single_table_only)

    def parse_table_factor(self, sql_statement, is_single_table_only):
        literals = self.lexer_engine.get_current_token().literals
        begin_position = self.lexer_engine.get_current_token().end_position - len(literals)
        self.lexer_engine.next_token()
        if self.lexer_engine.equal_any(Symbol.DOT):
            raise UnsupportedOperationException("Cannot support for 'schema.table'")
        table_name = sqlutil.get_exactly_value(literals)
        if not table_name:
            return
        alias = self.alias_expression_parser.parse_table_alias()
        if is_single_table_only or self.sharding_rule.try_find_table_rule(
                table_name) or self.sharding_rule.find_binding_table_rule(
            table_name) or self.sharding_rule.default_datasource_name in self.sharding_rule.data_source_map:
            sql_statement.sql_tokens.append(TableToken(begin_position, literals))
            sql_statement.tables.add(Table(table_name, alias))
        self._parse_join_table(sql_statement)
        if is_single_table_only and not sql_statement.tables.is_single_table():
            raise UnsupportedOperationException("Cannot support Multiple-Table")

    def _parse_join_table(self, sql_statement):
        while self._parse_join_type():
            if self.lexer_engine.equal_any(Symbol.LEFT_PAREN):
                raise UnsupportedOperationException("Cannot support sub query for join table")
            self.parse_table_factor(sql_statement, False)
            self._parse_join_condition(sql_statement)

    def _parse_join_type(self):
        join_type_keywords = [DefaultKeyword.INNER, DefaultKeyword.OUTER, DefaultKeyword.LEFT, DefaultKeyword.RIGHT,
                              DefaultKeyword.FULL, DefaultKeyword.CROSS, DefaultKeyword.NATURAL, DefaultKeyword.JOIN]
        join_type_keywords.extend(self.get_keywords_for_join_type())
        if not self.lexer_engine.equal_any(*join_type_keywords):
            return False
        self.lexer_engine.skip_all(join_type_keywords)
        return True

    def get_keywords_for_join_type(self):
        return []

    def _parse_join_condition(self, sql_statement):
        if self.lexer_engine.skip_if_equal(DefaultKeyword.ON):
            while True:
                self.basic_expression_parser.parse(sql_statement)
                self.lexer_engine.accept(Symbol.EQ)
                self.basic_expression_parser.parse(sql_statement)
                if self.lexer_engine.skip_if_equal(DefaultKeyword.AND):
                    break
        elif self.lexer_engine.skip_if_equal(DefaultKeyword.USING):
            self.lexer_engine.skip_parenthese(sql_statement)


class WhereClauseParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine
        self.alias_expression_parser = create_alias_expression_parser(lexer_engine)
        self.basic_expression_parser = create_basic_expression_parser(lexer_engine)

    def parse(self, sharding_rule, sql_statement, select_items):
        self.alias_expression_parser.parse_table_alias()
        if self.lexer_engine.skip_if_equal(DefaultKeyword.WHERE):
            self._parse_conditions(sharding_rule, sql_statement, select_items)

    def _parse_conditions(self, sharding_rule, sql_statement, select_items):
        while True:
            self._parse_comparison_condition(sharding_rule, sql_statement, select_items)
            if not self.lexer_engine.skip_if_equal(DefaultKeyword.AND):
                break
        self.lexer_engine.unsupported_if_equal(DefaultKeyword.OR)

    def _parse_comparison_condition(self, sharding_rule, sql_statement, select_items):
        self.lexer_engine.skip_if_equal(Symbol.LEFT_PAREN)
        left = self.basic_expression_parser.parse(sql_statement)
        if self.lexer_engine.skip_if_equal(Symbol.EQ):
            self._parse_equal_condition(sharding_rule, sql_statement, left)
            self.lexer_engine.skip_if_equal(Symbol.RIGHT_PAREN)
            return
        if self.lexer_engine.skip_if_equal(DefaultKeyword.IN):
            self._parse_in_condition(sharding_rule, sql_statement, left)
            self.lexer_engine.skip_if_equal(Symbol.RIGHT_PAREN)
            return
        if self.lexer_engine.skip_if_equal(DefaultKeyword.BETWEEN):
            self._parse_between_condition(sharding_rule, sql_statement, left)
            self.lexer_engine.skip_if_equal(Symbol.RIGHT_PAREN)
            return

        other_condition_operators = [Symbol.LT, Symbol.LT_EQ, Symbol.GT, Symbol.GT_EQ, Symbol.LT_GT, Symbol.BANG_EQ,
                                     Symbol.BANG_GT, Symbol.BANG_LT, DefaultKeyword.LIKE, DefaultKeyword.IS]
        other_condition_operators.extend(self.get_customized_other_condition_operators())
        if self.lexer_engine.skip_if_equal(*other_condition_operators):
            self._parse_other_condition(sql_statement)

        if self.skip_if_equal(DefaultKeyword.NOT):
            self.lexer_engine.next_token()
            self.lexer_engine.skip_if_equal(Symbol.LEFT_PAREN)
            self._parse_other_condition(sql_statement)
            self.lexer_engine.skip_if_equal(Symbol.RIGHT_PAREN)

        self.lexer_engine.skip_if_equal(Symbol.RIGHT_PAREN)

    def get_customized_other_condition_operators(self):
        return []

    def _parse_other_condition(self, sql_statement):
        self.basic_expression_parser.parse(sql_statement)

    def _parse_equal_condition(self, sharding_rule, sql_statement, left):
        right = self.basic_expression_parser.parse(sql_statement)
        if (sql_statement.tables.is_single_table() or isinstance(left, SQLPropertyExpression)) and (
                        isinstance(right, SQLPlaceholderExpression) or isinstance(right,
                                                                                  SQLNumberExpression) or isinstance(
                    right, SQLTextExpression)):
            column = self._find(sql_statement.tables, left)
            if column:
                sql_statement.conditions.add(Condition(column, ShardingOperator.EQUAL, right), sharding_rule)

    def _parse_in_condition(self, sharding_rule, sql_statement, left):
        self.lexer_engine.accept(Symbol.LEFT_PAREN)
        rights = []
        while True:
            self.lexer_engine.skip_if_equal(Symbol.COMMA)
            rights.append(self.basic_expression_parser.parse(sql_statement))
            if self.lexer_engine.equal_any(Symbol.RIGHT_PAREN):
                break
        column = self._find(sql_statement.tables, left)
        if column:
            sql_statement.conditions.add(Condition(column, ShardingOperator.IN, *rights), sharding_rule)
        self.lexer_engine.next_token()

    def _parse_between_condition(self, sharding_rule, sql_statement, left):
        rights = []
        rights.append(self.basic_expression_parser.parse(sql_statement))
        self.lexer_engine.accept(DefaultKeyword.AND)
        rights.append(self.basic_expression_parser.parse(sql_statement))
        column = self._find(sql_statement.tables, left)
        if column:
            sql_statement.conditions.add(Condition(column, ShardingOperator.BETWEEN, *rights), sharding_rule)

    def _find(self, tables, sql_expression):
        if isinstance(sql_expression, SQLPropertyExpression):
            return self._get_column_with_owner(tables, sql_expression)
        if isinstance(sql_expression, SQLIdentifierExpression):
            return self._get_column_without_owner(tables, sql_expression)

    def _get_column_with_owner(self, tables, property_expression):
        table = tables.find(sqlutil.get_exactly_value(property_expression.owner.name))
        if table:
            return Column(sqlutil.get_exactly_value(property_expression.name), table.name)

    def _get_column_without_owner(self, tables, identifier_expression):
        if tables.is_single_table():
            return Column(sqlutil.get_exactly_value(identifier_expression.name), tables.get_single_table_name())


class GroupByClauseParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine
        self.basic_expression_parser = create_basic_expression_parser(lexer_engine)

    def parse(self, select_statement):
        if not self.lexer_engine.skip_if_equal(DefaultKeyword.GROUP):
            return
        self.lexer_engine.skip_if_equal(DefaultKeyword.BY)

        while True:
            self._add_group_by_item(self.basic_expression_parser.parse(select_statement), select_statement)
            if not self.lexer_engine.equal_any(Symbol.COMMA):
                break
            self.lexer_engine.next_token()
        self.lexer_engine.skip_all(*self.get_skipped_keyword_after_group_by())
        select_statement.group_by_last_position = self.lexer_engine.get_current_token().end_position - len(
            self.lexer_engine.get_current_token().literals)

    def _add_group_by_item(self, sql_expr, select_statement):
        self.lexer_engine.unsupported_if_equal(*self.get_unsupported_keyword_before_group_by_item())
        order_by_type = OrderType.ASC
        if self.lexer_engine.equal_any(DefaultKeyword.ASC):
            self.lexer_engine.next_token()
        elif self.lexer_engine.skip_if_equal(DefaultKeyword.DESC):
            order_by_type = OrderType.DESC

        if isinstance(sql_expr, SQLPropertyExpression):
            order_item = OrderItem(sqlutil.get_exactly_value(sql_expr.owner.name),
                                   sqlutil.get_exactly_value(sql_expr.name), order_by_type, OrderType.ASC,
                                   select_statement.get_alias(
                                       sqlutil.get_exactly_value(sql_expr.owner.name) + '.' + sqlutil.get_exactly_value(
                                           sql_expr.name)))
        elif isinstance(sql_expr, SQLIdentifierExpression):
            order_item = OrderItem(None, sqlutil.get_exactly_value(sql_expr.name),
                                   order_by_type, OrderType.ASC,
                                   select_statement.get_alias(sqlutil.get_exactly_value(sql_expr.name)))
        elif isinstance(sql_expr, SQLIgnoreExpression):
            order_item = OrderItem(None, sqlutil.get_exactly_value(sql_expr.name),
                                   order_by_type, OrderType.ASC,
                                   select_statement.get_alias(sqlutil.get_exactly_value(sql_expr.expression)))
        else:
            return
        select_statement.group_by_items.add(order_item)

    def get_unsupported_keyword_before_group_by_item(self):
        return []

    def get_skipped_keyword_after_group_by(self):
        return []


class HavingClauseParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine

    def parse(self):
        self.lexer_engine.unsupported_if_equal(DefaultKeyword.HAVING)


class OrderByClauseParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine
        self.basic_expression_parser = create_basic_expression_parser(lexer_engine)

    def parse(self, select_statement):
        if not self.lexer_engine.skip_if_equal(DefaultKeyword.ORDER):
            return
        result = []
        self.lexer_engine.accept(DefaultKeyword.BY)
        while True:
            result.append(self._parse_select_order_by_item(select_statement))
            if not self.lexer_engine.skip_if_equal(Symbol.COMMA):
                break
        select_statement.order_by_items.extend(result)

    def _parse_select_order_by_item(self, select_statement):
        sql_expr = self.basic_expression_parser.parse(select_statement)
        order_by_type = OrderType.ASC
        if self.lexer_engine.skip_if_equal(DefaultKeyword.ASC):
            order_by_type = OrderType.ASC
        elif self.lexer_engine.skip_if_equal(DefaultKeyword.DESC):
            order_by_type = OrderType.DESC

        if isinstance(sql_expr, SQLNumberExpression):
            return OrderItem(None, None, order_by_type, OrderType.ASC, None, sql_expr.number)
        if isinstance(sql_expr, SQLPropertyExpression):
            return OrderItem(sqlutil.get_exactly_value(sql_expr.owner.name),
                             sqlutil.get_exactly_value(sql_expr.name), order_by_type, OrderType.ASC,
                             select_statement.get_alias(
                                 sqlutil.get_exactly_value(sql_expr.owner.name) + '.' + sqlutil.get_exactly_value(
                                     sql_expr.name)))
        if isinstance(sql_expr, SQLIdentifierExpression):
            return OrderItem(None, sqlutil.get_exactly_value(sql_expr.name),
                             order_by_type, OrderType.ASC,
                             select_statement.get_alias(sqlutil.get_exactly_value(sql_expr.name)))
        if isinstance(sql_expr, SQLIgnoreExpression):
            return OrderItem(None, sqlutil.get_exactly_value(sql_expr.name),
                             order_by_type, OrderType.ASC,
                             select_statement.get_alias(sqlutil.get_exactly_value(sql_expr.expression)))
        raise SQLParsingException(self.lexer_engine)


class SelectRestClauseParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine

    def parse(self):
        unsupported_rest_keywords = [DefaultKeyword.UNION, DefaultKeyword.INTERSECT, DefaultKeyword.EXCEPT,
                                     DefaultKeyword.MINUS, *self.get_unsupported_keywords_rest()]
        self.lexer_engine.unsupported_if_equal(*unsupported_rest_keywords)

    def get_unsupported_keywords_rest(self):
        return []