from shardingpy.constant import AggregationType, ShardingOperator, OrderDirection
from shardingpy.exception import UnsupportedOperationException, SQLParsingException, ShardingException
from shardingpy.parsing.lexer.token import DefaultKeyword, Symbol, Assist
from shardingpy.parsing.parser.context.condition import Column, Condition, OrCondition, AndCondition, NullCondition, \
    GeneratedKeyCondition
from shardingpy.parsing.parser.context.insertvalue import InsertValue
from shardingpy.parsing.parser.context.others import OrderItem
from shardingpy.parsing.parser.context.selectitem import StarSelectItem, AggregationSelectItem, CommonSelectItem
from shardingpy.parsing.parser.context.table import Table
from shardingpy.parsing.parser.dialect.expression_parser_factory import create_alias_expression_parser, \
    create_basic_expression_parser
from shardingpy.parsing.parser.expressionparser import SQLPropertyExpression, SQLPlaceholderExpression, \
    SQLNumberExpression, SQLTextExpression, SQLIdentifierExpression, SQLIgnoreExpression
from shardingpy.parsing.parser.token import TableToken, InsertColumnToken, ItemsToken, InsertValuesToken
from shardingpy.util import sqlutil, strutil


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
        elif self._is_aggregation_select_item():
            result = self._parse_aggregation_select_item(select_statement)
            self._parse_rest_select_item(select_statement)
        else:
            result = self._parse_common_or_star_select_item(select_statement)
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

    def _parse_common_or_star_select_item(self, select_statement):
        result = ""
        literals = self.lexer_engine.get_current_token().literals
        position = self.lexer_engine.get_current_token().end_position - len(literals)
        result += literals
        self.lexer_engine.next_token()
        if self.lexer_engine.equal_any(Symbol.LEFT_PAREN):
            result += self.lexer_engine.skip_parentheses(select_statement)
        elif self.lexer_engine.equal_any(Symbol.DOT):
            table_name = sqlutil.get_exactly_value(literals)
            if self.sharding_rule.try_find_table_rule_by_logic_table(
                    table_name) or self.sharding_rule.find_binding_table_rule(table_name):
                select_statement.sql_tokens.append(TableToken(position, 0, literals))
            result += self.lexer_engine.get_current_token().literals
            self.lexer_engine.next_token()
            if self.lexer_engine.equal_any(Symbol.STAR):
                return self._parse_star_select_item()
            result += self.lexer_engine.get_current_token().literals
            self.lexer_engine.next_token()
        return CommonSelectItem(sqlutil.get_exactly_value(result) + self._parse_rest_select_item(select_statement),
                                self.alias_expression_parser.parse_select_item_alias())

    def _parse_rest_select_item(self, select_statement):
        result = ""
        while self.lexer_engine.equal_any(*Symbol.operators()):
            result += self.lexer_engine.get_current_token().literals
            self.lexer_engine.next_token()
            select_item = self._parse_common_or_star_select_item(select_statement)
            result += select_item.expression
        return result


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
        skipped_schema_name_length = 0
        self.lexer_engine.next_token()
        if self.lexer_engine.skip_if_equal(Symbol.DOT):
            skipped_schema_name_length += len(literals) + len(Symbol.DOT.value)
            literals = self.lexer_engine.get_current_token().literals
        table_name = sqlutil.get_exactly_value(literals)
        if not table_name:
            return
        alias = self.alias_expression_parser.parse_table_alias()
        if is_single_table_only or self.sharding_rule.try_find_table_rule_by_logic_table(
                table_name) or self.sharding_rule.find_binding_table_rule(
            table_name) or self.sharding_rule.sharding_data_source_names.get_default_data_source_name() in \
                self.sharding_rule.sharding_data_source_names.data_source_names:
            sql_statement.sql_tokens.append(TableToken(begin_position, skipped_schema_name_length, literals))
            sql_statement.tables.add(Table(table_name, alias))
        self._parse_force_index(table_name, sql_statement)
        self._parse_join_table(sql_statement)
        if is_single_table_only and not sql_statement.tables.is_single_table():
            raise UnsupportedOperationException("Cannot support Multiple-Table")

    def _parse_force_index(self, table_name, sql_statement):
        # TODO force index
        pass

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
        self.lexer_engine.skip_all(*join_type_keywords)
        return True

    def get_keywords_for_join_type(self):
        return []

    def _parse_join_condition(self, sql_statement):
        if self.lexer_engine.skip_if_equal(DefaultKeyword.ON):
            while True:
                self.basic_expression_parser.parse(sql_statement)
                self.lexer_engine.accept(Symbol.EQ)
                self.basic_expression_parser.parse(sql_statement)
                if not self.lexer_engine.skip_if_equal(DefaultKeyword.AND):
                    break
        elif self.lexer_engine.skip_if_equal(DefaultKeyword.USING):
            self.lexer_engine.skip_parentheses(sql_statement)


class WhereClauseParser:
    def __init__(self, database_type, lexer_engine):
        self.database_type = database_type
        self.lexer_engine = lexer_engine
        self.alias_expression_parser = create_alias_expression_parser(lexer_engine)
        self.basic_expression_parser = create_basic_expression_parser(lexer_engine)

    def parse(self, sharding_rule, sql_statement, select_items):
        self.alias_expression_parser.parse_table_alias()
        if self.lexer_engine.skip_if_equal(DefaultKeyword.WHERE):
            self._parse_where(sharding_rule, sql_statement, select_items)

    def _parse_where(self, sharding_rule, sql_statement, select_items):
        or_condition = self._parse_or(sharding_rule, sql_statement, select_items)
        if not (len(or_condition.and_conditions) == 1 and isinstance(or_condition.and_conditions[0].conditions[0],
                                                                     NullCondition)):
            sql_statement.conditions.or_condition.and_conditions.extend(or_condition.and_conditions)

    def _parse_or(self, sharding_rule, sql_statement, select_items):
        result = OrCondition()
        while True:
            if self.lexer_engine.skip_if_equal(Symbol.LEFT_PAREN):
                sub_or_condition = self._parse_or(sharding_rule, sql_statement, select_items)
                self.lexer_engine.skip_if_equal(Symbol.RIGHT_PAREN)
                or_condition = None
                result.and_conditions.extend(self._merge_or(sub_or_condition, or_condition).and_condtions)
            else:
                or_condition = self._parse_and(sharding_rule, sql_statement, select_items)
                result.and_conditions.extend(or_condition.and_conditions)
            if not self.lexer_engine.skip_if_equal(DefaultKeyword.OR):
                break
        return result

    def _parse_and(self, sharding_rule, sql_statement, select_items):
        result = OrCondition()
        while True:
            if self.lexer_engine.skip_if_equal(Symbol.LEFT_PAREN):
                sub_or_condition = self._parse_or(sharding_rule, sql_statement, select_items)
                self.lexer_engine.skip_if_equal(Symbol.RIGHT_PAREN)
                result = self._merge_or(result, sub_or_condition)
            else:
                condition = self._parse_comparison_condition(sharding_rule, sql_statement, select_items)
                self._skip_double_colon()
                result = self._merge_or(result, OrCondition(condition))
            if not self.lexer_engine.skip_if_equal(DefaultKeyword.AND):
                break
        return result

    def _merge_or(self, or_condition1, or_condition2):
        if not or_condition1 or not or_condition1.and_conditions:
            return or_condition2
        if not or_condition2 or not or_condition2.and_conditions:
            return or_condition1
        result = OrCondition()
        for each1 in or_condition1.and_conditions:
            for each2 in or_condition2.and_conditions:
                result.and_conditions.append(self._merge_and(each1, each2))
        return result

    def _merge_and(self, and_condition1, and_condition2):
        result = AndCondition()
        result.conditions.extend(and_condition1.conditions)
        result.conditions.extend(and_condition2.conditions)
        return result.optimize()

    def _parse_comparison_condition(self, sharding_rule, sql_statement, select_items):
        left = self.basic_expression_parser.parse(sql_statement)
        if self.lexer_engine.skip_if_equal(Symbol.EQ):
            return self._parse_equal_condition(sharding_rule, sql_statement, left)
        if self.lexer_engine.skip_if_equal(DefaultKeyword.IN):
            return self._parse_in_condition(sharding_rule, sql_statement, left)
        if self.lexer_engine.skip_if_equal(DefaultKeyword.BETWEEN):
            return self._parse_between_condition(sharding_rule, sql_statement, left)

        result = NullCondition();
        other_condition_operators = [Symbol.LT, Symbol.LT_EQ, Symbol.GT, Symbol.GT_EQ, Symbol.LT_GT, Symbol.BANG_EQ,
                                     Symbol.BANG_GT, Symbol.BANG_LT, DefaultKeyword.LIKE, DefaultKeyword.IS]
        other_condition_operators.extend(self.get_customized_other_condition_operators())
        if self.lexer_engine.skip_if_equal(*other_condition_operators):
            self.lexer_engine.skip_if_equal(DefaultKeyword.NOT)
            self._parse_other_condition(sql_statement)

        if self.lexer_engine.skip_if_equal(DefaultKeyword.NOT):
            self._parse_not_condition(sql_statement)

        return result

    def get_customized_other_condition_operators(self):
        return []

    def _parse_other_condition(self, sql_statement):
        self.basic_expression_parser.parse(sql_statement)

    def _parse_not_condition(self, sql_statement):
        if self.lexer_engine.skip_if_equal(DefaultKeyword.BETWEEN):
            self._parse_other_condition(sql_statement)
            self._skip_double_colon()
            self.lexer_engine.skip_if_equal(DefaultKeyword.AND)
            self._parse_other_condition(sql_statement)
            return
        if self.lexer_engine.skip_if_equal(DefaultKeyword.IN):
            self.lexer_engine.accept(Symbol.LEFT_PAREN)
            while True:
                self._parse_other_condition(sql_statement)
                self._skip_double_colon()
                if not self.lexer_engine.skip_if_equal(Symbol.COMMA):
                    break
            self.lexer_engine.accept(Symbol.RIGHT_PAREN)
        else:
            self.lexer_engine.next_token()
            self._parse_other_condition(sql_statement)

    def _parse_equal_condition(self, sharding_rule, sql_statement, left):
        right = self.basic_expression_parser.parse(sql_statement)
        if (sql_statement.tables.is_single_table() or isinstance(left, SQLPropertyExpression)) and (
                isinstance(right, SQLPlaceholderExpression) or isinstance(right,
                                                                          SQLNumberExpression) or isinstance(
            right, SQLTextExpression)):
            column = self._find(sql_statement.tables, left)
            if column and sharding_rule.is_sharding_column(column):
                return Condition(column, ShardingOperator.EQUAL, right)
        return NullCondition()

    def _parse_in_condition(self, sharding_rule, sql_statement, left):
        self.lexer_engine.accept(Symbol.LEFT_PAREN)
        rights = list()
        while True:
            rights.append(self.basic_expression_parser.parse(sql_statement))
            self._skip_double_colon()
            if not self.lexer_engine.skip_if_equal(Symbol.COMMA):
                break
        self.lexer_engine.accept(Symbol.RIGHT_PAREN)
        column = self._find(sql_statement.tables, left)
        if column and sharding_rule.is_sharding_column(column):
            return Condition(column, ShardingOperator.IN, *rights)
        return NullCondition()

    def _parse_between_condition(self, sharding_rule, sql_statement, left):
        rights = list()
        rights.append(self.basic_expression_parser.parse(sql_statement))
        self._skip_double_colon()
        self.lexer_engine.accept(DefaultKeyword.AND)
        rights.append(self.basic_expression_parser.parse(sql_statement))
        column = self._find(sql_statement.tables, left)
        if column and sharding_rule.is_sharding_column(column):
            return Condition(column, ShardingOperator.BETWEEN, *rights)
        return NullCondition()

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

    def _skip_double_colon(self):
        if self.lexer_engine.skip_if_equal(Symbol.DOUBLE_COLON):
            self.lexer_engine.next_token()


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
        order_by_type = OrderDirection.ASC
        if self.lexer_engine.equal_any(DefaultKeyword.ASC):
            self.lexer_engine.next_token()
        elif self.lexer_engine.skip_if_equal(DefaultKeyword.DESC):
            order_by_type = OrderDirection.DESC

        if isinstance(sql_expr, SQLPropertyExpression):
            order_item = OrderItem(sqlutil.get_exactly_value(sql_expr.owner.name),
                                   sqlutil.get_exactly_value(sql_expr.name), order_by_type, OrderDirection.ASC,
                                   select_statement.get_alias(
                                       sqlutil.get_exactly_value(sql_expr.owner.name) + '.' + sqlutil.get_exactly_value(
                                           sql_expr.name)))
        elif isinstance(sql_expr, SQLIdentifierExpression):
            order_item = OrderItem(None, sqlutil.get_exactly_value(sql_expr.name),
                                   order_by_type, OrderDirection.ASC,
                                   select_statement.get_alias(sqlutil.get_exactly_value(sql_expr.name)))
        elif isinstance(sql_expr, SQLIgnoreExpression):
            order_item = OrderItem(None, sql_expr.expression,
                                   order_by_type, OrderDirection.ASC,
                                   select_statement.get_alias(sql_expr.expression))
        else:
            return
        select_statement.group_by_items.append(order_item)

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
        order_by_type = OrderDirection.ASC
        if self.lexer_engine.skip_if_equal(DefaultKeyword.ASC):
            order_by_type = OrderDirection.ASC
        elif self.lexer_engine.skip_if_equal(DefaultKeyword.DESC):
            order_by_type = OrderDirection.DESC

        if isinstance(sql_expr, SQLNumberExpression):
            return OrderItem(None, None, order_by_type, OrderDirection.ASC, None, sql_expr.number)
        if isinstance(sql_expr, SQLIdentifierExpression):
            return OrderItem(None, sqlutil.get_exactly_value(sql_expr.name),
                             order_by_type, OrderDirection.ASC,
                             select_statement.get_alias(sqlutil.get_exactly_value(sql_expr.name)))
        if isinstance(sql_expr, SQLPropertyExpression):
            return OrderItem(sqlutil.get_exactly_value(sql_expr.owner.name),
                             sqlutil.get_exactly_value(sql_expr.name), order_by_type, OrderDirection.ASC,
                             select_statement.get_alias(
                                 sqlutil.get_exactly_value(sql_expr.owner.name) + '.' + sqlutil.get_exactly_value(
                                     sql_expr.name)))
        if isinstance(sql_expr, SQLIgnoreExpression):
            return OrderItem(None, sqlutil.get_exactly_value(sql_expr.expression),
                             order_by_type, OrderDirection.ASC,
                             select_statement.get_alias(sql_expr.expression))
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


class InsertIntoClauseParser:
    def __init__(self, lexer_engine, table_references_clause_parser):
        self.lexer_engine = lexer_engine
        self.table_references_clause_parser = table_references_clause_parser

    def parse(self, insert_statement):
        self.lexer_engine.unsupported_if_equal(*self.get_unsupported_keywords_before_into())
        self.lexer_engine.skip_until(DefaultKeyword.INTO)
        self.lexer_engine.next_token()
        self.table_references_clause_parser.parse(insert_statement, True)
        self._skip_between_table_and_values(insert_statement)

    def get_unsupported_keywords_before_into(self):
        return []

    def _skip_between_table_and_values(self, insert_statement):
        while self.lexer_engine.skip_if_equal(*self.get_skipped_keywords_between_table_and_values()):
            self.lexer_engine.next_token()
            if self.lexer_engine.equal_any(Symbol.LEFT_PAREN):
                self.lexer_engine.skip_parentheses(insert_statement)

    def get_skipped_keywords_between_table_and_values(self):
        return []


class InsertColumnsClauseParser:
    def __init__(self, sharding_rule, lexer_engine):
        self.sharding_rule = sharding_rule
        self.lexer_engine = lexer_engine

    def parse(self, insert_statement, sharding_meta_data):
        result = list()
        table_name = insert_statement.tables.get_single_table_name()
        generated_key_column = self.sharding_rule.get_generate_key_column(table_name)
        count = 0
        if self.lexer_engine.equal_any(Symbol.LEFT_PAREN):
            while True:
                self.lexer_engine.next_token()
                column_name = sqlutil.get_exactly_value(self.lexer_engine.get_current_token().literals)
                result.append(Column(column_name, table_name))
                self.lexer_engine.next_token()
                if generated_key_column and strutil.equals_ignore_case(generated_key_column.name, column_name):
                    insert_statement.generate_key_column_index = count
                count += 1
                if self.lexer_engine.equal_any(Symbol.RIGHT_PAREN) or self.lexer_engine.equal_any(Assist.END):
                    break
            insert_statement.columns_list_last_position = self.lexer_engine.get_current_token().end_position - len(
                self.lexer_engine.get_current_token().literals)
            self.lexer_engine.next_token()
        else:
            column_names = sharding_meta_data.table_meta_data_map.get(table_name).get_all_column_names()
            begin_position = self.lexer_engine.get_current_token().end_position - len(
                self.lexer_engine.get_current_token().literals) - 1
            insert_statement.sql_tokens.append(InsertColumnToken(begin_position, '('))
            columns_token = ItemsToken(begin_position)
            columns_token.is_first_of_items_special = True
            for column_name in column_names:
                result.append(Column(column_name, table_name))
                if generated_key_column and strutil.equals_ignore_case(generated_key_column.name, column_name):
                    insert_statement.generate_key_column_index = count
                columns_token.items.append(column_name)
                count += 1
            insert_statement.sql_tokens.append(columns_token)
            insert_statement.sql_tokens.append(InsertColumnToken(begin_position, ')'))
            insert_statement.columns_list_last_position = begin_position
        insert_statement.columns.extend(result)


class InsertValuesClauseParser:
    def __init__(self, sharding_rule, lexer_engine):
        self.sharding_rule = sharding_rule
        self.lexer_engine = lexer_engine
        self.basic_expression_parser = create_basic_expression_parser(lexer_engine)

    def get_synonymous_keywords_for_values(self):
        return []

    def parse(self, insert_statement):
        if self.lexer_engine.skip_if_equal(DefaultKeyword.VALUES, *self.get_synonymous_keywords_for_values()):
            self._parse_values(insert_statement)

    def _parse_values(self, insert_statement):
        begin_position = self.lexer_engine.get_current_token().end_position - len(
            self.lexer_engine.get_current_token().literals)
        insert_statement.sql_tokens.append(
            InsertValuesToken(begin_position, insert_statement.tables.get_single_table_name()))
        while True:
            begin_position = self.lexer_engine.get_current_token().end_position - len(
                self.lexer_engine.get_current_token().literals)
            self.lexer_engine.accept(Symbol.LEFT_PAREN)
            sql_expressions = list()
            columns_count = 0
            while True:
                sql_expressions.append(self.basic_expression_parser.parse(insert_statement))
                self._skip_double_colon()
                columns_count += 1
                if not self.lexer_engine.skip_if_equal(Symbol.COMMA):
                    break
            self._remove_generate_key_column(insert_statement, columns_count)
            columns_count = 0
            paramters_count = 0
            and_condition = AndCondition()
            for each in insert_statement.columns:
                sql_expression = sql_expressions[columns_count]
                if self.sharding_rule.is_sharding_column(each):
                    and_condition.conditions.append(Condition(each, ShardingOperator.EQUAL, sql_expression))
                if insert_statement.generate_key_column_index == columns_count:
                    insert_statement.generated_key_conditions.append(
                        self._create_generate_key_condition(each, sql_expression))
                columns_count += 1
                if isinstance(sql_expression, SQLPlaceholderExpression):
                    paramters_count += 1
            end_position = self.lexer_engine.get_current_token().end_position
            self.lexer_engine.accept(Symbol.RIGHT_PAREN)
            insert_statement.insert_values.insert_values.append(
                InsertValue(self.lexer_engine.get_sql()[begin_position: end_position], paramters_count))
            insert_statement.conditions.or_condition.and_conditions.append(and_condition)
            if not self.lexer_engine.skip_if_equal(Symbol.COMMA):
                break
        insert_statement.insert_values_list_last_position = end_position

    def _remove_generate_key_column(self, insert_statement, values_count):
        generate_key_column = self.sharding_rule.get_generate_key_column(
            insert_statement.tables.get_single_table_name())
        if generate_key_column and values_count < len(insert_statement.columns):
            insert_statement.columns.remove(
                Column(generate_key_column.name, insert_statement.tables.get_single_table_name()))
            for each in insert_statement.get_items_tokens():
                each.items.remove(generate_key_column.name)
                insert_statement.generate_key_column_index = -1

    def _create_generate_key_condition(self, column, sql_expression):
        if isinstance(sql_expression, SQLPlaceholderExpression):
            return GeneratedKeyCondition(column, sql_expression.index, None)
        elif isinstance(sql_expression, SQLNumberExpression):
            return GeneratedKeyCondition(column, -1, sql_expression.number)
        else:
            raise ShardingException('Generated key only support number.')

    def _skip_double_colon(self):
        if self.lexer_engine.skip_if_equal(Symbol.DOUBLE_COLON):
            self.lexer_engine.next_token()


class InsertSetClauseParser:
    def __init__(self, sharding_rule, lexer_engine):
        self.sharding_rule = sharding_rule
        self.lexer_engine = lexer_engine
        self.basic_expression_parser = create_basic_expression_parser(lexer_engine)

    def parse(self, insert_statement):
        if not self.lexer_engine.skip_if_equal(*self.get_customized_insert_keywords()):
            return

        raise NotImplementedError('not support "insert clause with set values"')

    def get_customized_insert_keywords(self):
        return []


class InsertDuplicateKeyUpdateClauseParser:
    def __init__(self, sharding_rule, lexer_engine):
        self.sharding_rule = sharding_rule
        self.lexer_engine = lexer_engine

    def parse(self, insert_statement):
        if not self.lexer_engine.skip_if_equal(*self.get_customized_insert_keywords()):
            return

        self.lexer_engine.accept(DefaultKeyword.DUPLICATE)
        self.lexer_engine.accept(DefaultKeyword.KEY)
        self.lexer_engine.accept(DefaultKeyword.UPDATE)
        while True:
            column = Column(self.lexer_engine.get_current_token().literals,
                            insert_statement.tables.get_single_table_name())
            if self.sharding_rule.is_sharding_column(column):
                raise SQLParsingException(
                    'INSERT INTO .... ON DUPLICATE KEY UPDATE can not support on sharding column: {}'.format(
                        column.name))
            self.lexer_engine.skip_until(Symbol.COMMA, Assist.END)
            if not self.lexer_engine.skip_if_equal(Symbol.COMMA):
                break

    def get_customized_insert_keywords(self):
        return []


class UpdateSetItemsClauseParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine
        self.basic_expression_parser = create_basic_expression_parser(lexer_engine)

    def parse(self, update_statement):
        self.lexer_engine.accept(DefaultKeyword.SET)
        while True:
            self._parse_set_item(update_statement)
            if not self.lexer_engine.skip_if_equal(Symbol.COMMA):
                break

    def _parse_set_item(self, update_statement):
        self._parse_set_column(update_statement)
        self.lexer_engine.skip_if_equal(Symbol.EQ, Symbol.COLON_EQ)
        self._parse_set_value(update_statement)
        self._skip_double_colon()

    def _parse_set_column(self, update_statement):
        if self.lexer_engine.equal_any(Symbol.LEFT_PAREN):
            self.lexer_engine.skip_parentheses(update_statement)
            return
        begin_position = self.lexer_engine.get_current_token().end_position
        literals = self.lexer_engine.get_current_token().literals
        self.lexer_engine.next_token()
        if self.lexer_engine.skip_if_equal(Symbol.DOT):
            if strutil.equals_ignore_case(update_statement.tables.get_single_table_name(),
                                          sqlutil.get_exactly_value(literals)):
                update_statement.sql_tokens.append(TableToken(begin_position - len(literals), 0, literals))
            self.lexer_engine.next_token()

    def _parse_set_value(self, update_statement):
        self.basic_expression_parser.parse(update_statement)

    def _skip_double_colon(self):
        if self.lexer_engine.skip_if_equal(Symbol.DOUBLE_COLON):
            self.lexer_engine.next_token()
