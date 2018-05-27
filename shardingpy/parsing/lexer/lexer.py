"""
Lexer
"""
from shardingpy.exception import SQLParsingException
from shardingpy.parsing.lexer.token import *


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
            self.current_token = Token(Assist.END, "", self.offset)
        else:
            raise SQLParsingException(self, Assist.ERROR)

        self.offset = self.current_token.end_position

    def get_current_token(self):
        return self.current_token

    def is_hint_begin(self):
        return False

    def is_comment_begin(self):
        ch = self.get_current_char(0)
        next_ch = self.get_current_char(1)
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
        return is_digital(self.get_current_char(0)) or ((self.get_current_char(0) == '.' and is_digital(
            self.get_current_char(1)) and not self._is_identifier_begin_with_char(self.get_current_char(-1))) or (
                                                                self.get_current_char(0) == '-' and (
                                                                self.get_current_char(1) == '.' or is_digital(
                                                            self.get_current_char(1)))))

    def _is_symbol_begin(self):
        return is_symbol(self.get_current_char(0))

    def _is_char_begin(self):
        return self.get_current_char(0) in "'\""

    def _is_end(self):
        return self.offset >= len(self.sql)

    def get_current_char(self, offset):
        return self.sql[self.offset + offset] if self.offset + offset < len(self.sql) else EOI
