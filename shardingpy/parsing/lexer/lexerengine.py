from shardingpy.constant import DatabaseType
from shardingpy.exception import SQLParsingException, SQLParsingUnsupportedException, UnsupportedOperationException
from shardingpy.parsing.lexer.dialect.mysql import MySQLLexer
from shardingpy.parsing.lexer.token import *


class LexerEngine:
    def __init__(self, lexer):
        self.lexer = lexer

    def get_sql(self):
        return self.lexer.sql

    def next_token(self):
        return self.lexer.next_token()

    def get_current_token(self):
        return self.lexer.get_current_token()

    def skip_parentheses(self, sql_statement):
        result = ''
        count = 0
        current_token = self.get_current_token()
        if Symbol.LEFT_PAREN == current_token.token_type:
            begin_position = current_token.end_position
            result += Symbol.LEFT_PAREN.value
            self.lexer.next_token()
            current_token = self.lexer.get_current_token()
            while True:
                if self.equal_any(Symbol.QUESTION):
                    sql_statement.increase_parameters_index()
                if current_token.token_type is Assist.END or (
                        current_token.token_type is Symbol.RIGHT_PAREN and count == 0):
                    break
                if current_token.token_type is Symbol.LEFT_PAREN:
                    count += 1
                elif current_token.token_type is Symbol.RIGHT_PAREN:
                    count -= 1
                self.lexer.next_token()
                current_token = self.lexer.get_current_token()
            result += self.lexer.sql[begin_position:current_token.end_position]
            self.lexer.next_token()
            return result

    def accept(self, token_type):
        if self.lexer.get_current_token().token_type != token_type:
            raise SQLParsingException(self.lexer, token_type)
        self.lexer.next_token()

    def equal_any(self, *token_types):
        for each in token_types:
            if self.lexer.get_current_token().token_type is each:
                return True
        return False

    def skip_if_equal(self, *token_types):
        if self.equal_any(*token_types):
            self.lexer.next_token()
            return True
        return False

    def skip_all(self, *token_types):
        while self.lexer.get_current_token().token_type in token_types:
            self.lexer.next_token()

    def skip_until(self, *token_types):
        token_types = set(token_types)
        token_types.add(Assist.END)
        while self.lexer.get_current_token().token_type not in token_types:
            self.lexer.next_token()

    def unsupported_if_equal(self, *token_types):
        if self.equal_any(*token_types):
            raise SQLParsingUnsupportedException(self.lexer.get_current_token().token_type)

    def get_database_type(self):
        if isinstance(self.lexer, MySQLLexer):
            return DatabaseType.MySQL
        raise UnsupportedOperationException("Cannot support lexer class: {}".format(self.lexer))


class LexerEngineFactory:
    @classmethod
    def new_instance(cls, db_type, sql):
        assert isinstance(db_type, DatabaseType)
        if db_type == DatabaseType.MySQL:
            return LexerEngine(MySQLLexer(sql))
        else:
            raise UnsupportedOperationException("Cannot support database {}".format(db_type))
