"""
Lexer
"""
from ..constant import DatabaseType
from .token import *
from .dialect.mysql import *
from ..exception import SQLParsingException, SQLParsingException, SQLParsingUnsupportedException


class Lexer:
    def __init__(self, sql, dictionary):
        self.sql = sql 
        self.dictionary = dictionary
        self.offset = 0
        self.current_token = None
        self.tokenizer = Tokenizer(sql, dictionary)

    def next_token(self):
        self._skip_ignored_token()
        if self.is_variable_begin():
            self.current_token = self.tokenizer.scan_variable()
        elif self._is_n_char_begin():
            self.offset += 1
            self.tokenizer.advance(1)
            self.current_token = self.tokenizer.scan_chars()
        elif self._is_identifier_begin():
            self.current_token = self.tokenizer.scan_identifier()
        elif self._is_hex_decimal_begin():
            self.current_token = self.tokenizer.scan_hex_decimal()
        elif self._is_number_begin():
            self.current_token = self.tokenizer.scan_number()
        elif self._is_symbol_begin():
            self.current_token = self.tokenizer.scan_symbol()
        elif self._is_char_begin():
            self.current_token = self.tokenizer.scan_chars()
        elif self._is_end():
            self.current_token = Token(Assit.END, "", self.offset)
        else:
            raise SQLParsingException("error token")

        self.offset = self.current_token.end_position

    def get_current_token(self):
        return self.current_token

    def is_hint_begin(self):
        return False

    def is_comment_begin(self):
        ch = self.get_current_char(0)
        next_ch= self.get_current_char(1)
        return (ch == '/' and next_ch == '/') or (ch == '-' and next_ch == '-') or (ch == '/' and next_ch == '*')

    def _skip_ignored_token(self):
        self.offset = self.tokenizer.skip_white_space()
        while self.is_hint_begin():
            self.offset = self.tokenizer.skip_hint()
            self.offset = self.tokenizer.skip_white_space()
        while self.is_comment_begin():
            self.offset = self.tokenizer.skip_comment()
            self.offset = self.tokenizer.skip_white_space()

    def is_variable_begin(self):
        return False

    def _is_n_char_begin(self):
        return self.is_support_n_chars() and self.get_current_char(0) == 'N' and self.get_current_char(1) == '\''

    def is_support_n_chars(self):
        return False

    def _is_identifier_begin(self):
        return self._is_identifier_begin_with_char(self.get_current_char(0))

    def _is_identifier_begin_with_char(self, ch):
        return is_alphabet(ch) or ch in "`_$"

    def _is_hex_decimal_begin(self):
        return self.get_current_char(0) == '0' and self.get_current_char(1) == 'x'

    def _is_number_begin(self):
        return is_digital(self.get_current_char(0)) or ((self.get_current_char(0) == '.' and is_digital(self.get_current_char(1)) and not self._is_identifier_begin_with_char(self.get_current_char(-1))) or (self.get_current_char(0) == '-' and (self.get_current_char(1) == '.' or is_digital(self.get_current_char(1)))))

    def _is_symbol_begin(self):
        return is_symbol(self.get_current_char(0))

    def _is_char_begin(self):
        return self.get_current_char(0) in "'\""

    def _is_end(self):
        return self.offset >= len(self.sql)

    def get_current_char(self, offset):
        return self.sql[self.offset + offset] if self.offset + offset < len(self.sql) else EOI


class LexerEngine:
    def __init__(self, lexer):
        self.lexer = lexer

    def get_sql(self):
        return self.lexer.get_sql()

    def next_token(self):
        return self.lexer.next_token()

    def get_current_token(self):
        return self.lexer.get_current_token()

    def skip_parentheses(self, sql_statement):
        result = ''
        count = 0
        current_token = self.get_current_token
        if Symbol.LEFT_PAREN == current_token.token_type:
            begin_position = current_token.end_position
            result += Symbol.LEFT_PAREN.value
            self.lexer.next_token()
            current_token = self.lexer.get_current_token()
            while True:
                if equal_any(Symbol.QUESTION):
                    sql_statement.increase_paremeters_index()
                if current_token.token_type == Assit.END or (current_token.token_type == Symbol.RIGHT_PAREN and count == 0):
                    break
                if current_token.token_type == Symbol.LEFT_PAREN:
                    count += 1
                elif current_token.token_type == Symbol.RIGHT_PAREN:
                    count -= 1
                self.lexer.next_token()
                current_token = self.lexer.get_currrent_token()
            result += self.lexer.get_sql[begin_position:current_token.end_position]
            self.lexer.next_token()
            return result

        def accept(self, token_type):
            if self.lexer.get_current_token.token_type != token_type:
                raise SQLParsingException(self.lexer, token_type)
            self.lexer.next_token()

        def equal_any(self, *token_types):
            for each in token_types:
                if self.lexer.get_current_token.token_type == each:
                    return True
            return False

        def skip_if_equal(self, *token_types):
            if self.equal_any(token_types):
                self.lexer.next_token()
                return True
            return False

        def skip_all(self, *token_types):
            while self.lexer.get_current_token().token_type in token_types:
                self.lexer.next_token()

        def skip_until(self, *token_types):
            token_types = set(token_types)
            token_types.add(Assit.END)
            while self.lexer.get_current_token().token_type not in token_types:
                self.lexer.next_token()

        def unsupported_if_equal(*token_types):
            if self.equal_any(token_types):
                raise SQLParsingUnsupportedException(self.lexer.get_current_token().token_type)


class LexerEnginerFactory:
    @classmethod
    def new_instance(cls, db_type, sql):
        assert isinstance(db_type, DatabaseType)
        if db_type == DatabaseType.MYSQL:
            return LexerEnginer(MySQLLexer(sql))
        else:
            raise UnsupportedOperationException("Cannot support database {}".format(db_type))

