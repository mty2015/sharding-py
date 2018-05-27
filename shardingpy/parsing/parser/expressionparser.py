# -*- coding: utf-8 -*-
from shardingpy.parsing.lexer.token import DefaultKeyword, Symbol, Literals
from shardingpy.parsing.parser.token import TableToken
from shardingpy.util import sqlutil


class SQLIdentifierExpression:
    def __init__(self, name):
        self.name = name


class SQLIgnoreExpression:
    def __init__(self, expression):
        self.expression = expression


class SQLNumberExpression:
    def __init__(self, number):
        self.number = number


class SQLPlaceholderExpression:
    def __init__(self, index):
        self.index = index


class SQLPropertyExpression:
    def __init__(self, owner, name):
        assert isinstance(owner, SQLIdentifierExpression)
        self.owner = owner
        self.name = name


class SQLTextExpression:
    def __init__(self, text):
        self.text = text


class AliasExpressionParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine

    def parse_select_item_alias(self):
        if self.lexer_engine.skip_if_equal(DefaultKeyword.AS):
            return self._parse_with_as()
        if self.lexer_engine.equal_any(self.get_default_available_keywords_for_select_item_alias().extend(
                self.get_customized_available_keywords_for_select_item_alias())):
            return self._parse_alias()

    def _parse_with_as(self):
        if self.lexer_engine.equal_any(*Symbol):
            return
        return self._parse_alias()

    def _parse_alias(self):
        result = sqlutil.get_exactly_value(self.lexer_engine.get_current_token().literals)
        self.lexer_engine.next_token()
        return result

    def get_default_available_keywords_for_select_item_alias(self):
        return [Literals.IDENTIFIER, Literals.CHARS, DefaultKeyword.TABLESPACE, DefaultKeyword.FUNCTION,
                DefaultKeyword.SEQUENCE, DefaultKeyword.OF, DefaultKeyword.DO, DefaultKeyword.NO,
                DefaultKeyword.TEMPORARY, DefaultKeyword.TEMP, DefaultKeyword.COMMENT, DefaultKeyword.AFTER,
                DefaultKeyword.INSTEAD, DefaultKeyword.ROW, DefaultKeyword.STATEMENT, DefaultKeyword.EXECUTE,
                DefaultKeyword.BITMAP, DefaultKeyword.NOSORT, DefaultKeyword.REVERSE, DefaultKeyword.COMPILE,
                DefaultKeyword.PASSWORD, DefaultKeyword.USER, DefaultKeyword.END, DefaultKeyword.CASE,
                DefaultKeyword.KEY, DefaultKeyword.INTERVAL,
                DefaultKeyword.CONSTRAINT]

    def get_customized_available_keywords_for_select_item_alias(self):
        return []

    def get_synonymous_keywords_for_distinct(self):
        return []

    def parse_table_alias(self):
        if self.lexer_engine.skip_if_equal(DefaultKeyword.AS):
            return self._parse_with_as()
        if self.lexer_engine.equal_any(self.get_default_available_keywords_for_table_alias().extend(
                self.get_customized_available_keywords_for_table_alias())):
            return self._parse_alias()

    def get_default_available_keywords_for_table_alias(self):
        return [Literals.IDENTIFIER, Literals.CHARS, DefaultKeyword.TABLESPACE, DefaultKeyword.FUNCTION,
                DefaultKeyword.SEQUENCE, DefaultKeyword.OF, DefaultKeyword.DO,
                DefaultKeyword.NO, DefaultKeyword.TEMPORARY, DefaultKeyword.TEMP, DefaultKeyword.COMMENT,
                DefaultKeyword.AFTER, DefaultKeyword.INSTEAD, DefaultKeyword.ROW,
                DefaultKeyword.STATEMENT, DefaultKeyword.EXECUTE, DefaultKeyword.BITMAP, DefaultKeyword.NOSORT,
                DefaultKeyword.REVERSE, DefaultKeyword.COMPILE,
                DefaultKeyword.PASSWORD, DefaultKeyword.USER, DefaultKeyword.END, DefaultKeyword.CASE,
                DefaultKeyword.KEY, DefaultKeyword.INTERVAL, DefaultKeyword.CONSTRAINT]

    def get_customized_available_keywords_for_table_alias(self):
        return []


class BasicExpressionParser:
    def __init__(self, lexer_engine):
        self.lexer_engine = lexer_engine

    def parse(self, sql_statement):
        begin_position = self.lexer_engine.get_current_token().end_position
        result = self._parse_expression(sql_statement)
        if isinstance(result, SQLPropertyExpression):
            self._set_table_token(sql_statement, begin_position, result)
        return result

    def _parse_expression(self, sql_statement):
        literals = self.lexer_engine.get_current_token().literals
        begin_position = self.lexer_engine.get_current_token().end_position - len(literals)
        expression = self._get_expression(literals, sql_statement)
        self.lexer_engine.next_token()
        if self.lexer_engine.skip_if_equal(Symbol.DOT):
            _property = self.lexer_engine.get_current_token().literals
            self.lexer_engine.next_token()
            if self._skip_if_composite_expression(sql_statement):
                return SQLIgnoreExpression(
                    self.lexer_engine.get_sql()[begin_position:self.lexer_engine.get_current_token().end_position])
            else:
                return SQLPropertyExpression(SQLIdentifierExpression(literals), _property)
        if self.lexer_engine.equal_any(Symbol.LEFT_PAREN):
            self.lexer_engine.skip_parentheses(sql_statement)
            self._skip_rest_composite_expression(sql_statement)
            return SQLIgnoreExpression(self.lexer_engine.get_sql()[
                                       begin_position:self.lexer_engine.get_current_token().end_position - len(
                                           self.lexer_engine.get_current_token().literals)])
        return SQLIgnoreExpression(self.lexer_engine.get_sql()[
                                   begin_position:self.lexer_engine.get_current_token().end_position]) if self._skip_if_composite_expression(
            sql_statement) else expression

    def _get_expression(self, literals, sql_statement):
        if self.lexer_engine.equal_any(Symbol.QUESTION):
            sql_statement.increase_parameters_index()
            return SQLPlaceholderExpression(sql_statement.parameters_index - 1)
        if self.lexer_engine.equal_any(Literals.CHARS):
            return SQLTextExpression(literals)
        if self.lexer_engine.equal_any(Literals.INT):
            return SQLNumberExpression(int(literals))
        if self.lexer_engine.equal_any(Literals.FLOAT):
            return SQLNumberExpression(float(literals))
        if self.lexer_engine.equal_any(Literals.HEX):
            return SQLNumberExpression(int(literals, 16))
        return SQLIgnoreExpression(literals)

    def _skip_if_composite_expression(self, sql_statement):
        if self.lexer_engine.equal_any(Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.PERCENT, Symbol.AMP,
                                       Symbol.BAR, Symbol.DOUBLE_AMP, Symbol.DOUBLE_BAR, Symbol.CARET, Symbol.DOT,
                                       Symbol.LEFT_PAREN):
            self.lexer_engine.skip_parentheses(sql_statement)
            self._skip_rest_composite_expression(sql_statement)
            return True
        return False

    def _skip_rest_composite_expression(self, sql_statement):
        while self.lexer_engine.skip_if_equal(Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.PERCENT,
                                              Symbol.AMP, Symbol.BAR, Symbol.DOUBLE_AMP, Symbol.DOUBLE_BAR,
                                              Symbol.CARET, Symbol.DOT):
            if self.lexer_engine.equal_any(Symbol.QUESTION):
                sql_statement.increase_parameters_index()
                self.lexer_engine.next_token()
                self.lexer_engine.skip_parentheses(sql_statement)

    def _set_table_token(self, sql_statement, begin_position, property_expr):
        owner = property_expr.owner.name
        if sqlutil.get_exactly_value(owner) in sql_statement.tables.get_table_names():
            sql_statement.sql_tokens.append(TableToken(begin_position - len(owner), owner))
