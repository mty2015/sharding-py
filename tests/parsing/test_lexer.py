import unittest

from shardingpy.exception import SQLParsingException
from shardingpy.parsing.lexer.dialect.mysql import MySQLLexer
from shardingpy.parsing.lexer.lexer import Lexer
from shardingpy.parsing.lexer.token import *


class LexerTestCase(unittest.TestCase):
    dictionary = Dictionary()

    def test_next_token_for_white_space(self):
        lexer = Lexer("Select * from \r\n TABLE_XXX \t", LexerTestCase.dictionary)
        self.assert_next_token(lexer, DefaultKeyword.SELECT, "Select")
        self.assert_next_token(lexer, Symbol.STAR, "*")
        self.assert_next_token(lexer, DefaultKeyword.FROM, "from")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "TABLE_XXX")
        self.assert_next_token(lexer, Assist.END, "")

    def test_next_token_for_order_by(self):
        lexer = Lexer("SELECT * FROM ORDER ORDER \t BY XX DESC", LexerTestCase.dictionary)
        self.assert_next_token(lexer, DefaultKeyword.SELECT, "SELECT")
        self.assert_next_token(lexer, Symbol.STAR, "*")
        self.assert_next_token(lexer, DefaultKeyword.FROM, "FROM")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "ORDER")
        self.assert_next_token(lexer, DefaultKeyword.ORDER, "ORDER")
        self.assert_next_token(lexer, DefaultKeyword.BY, "BY")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "XX")
        self.assert_next_token(lexer, DefaultKeyword.DESC, "DESC")
        self.assert_next_token(lexer, Assist.END, "")

    def test_next_token_for_group_by(self):
        lexer = Lexer("SELECT * FROM `XXX` GROUP BY XX DESC", LexerTestCase.dictionary)
        self.assert_next_token(lexer, DefaultKeyword.SELECT, "SELECT")
        self.assert_next_token(lexer, Symbol.STAR, "*")
        self.assert_next_token(lexer, DefaultKeyword.FROM, "FROM")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "`XXX`")
        self.assert_next_token(lexer, DefaultKeyword.GROUP, "GROUP")
        self.assert_next_token(lexer, DefaultKeyword.BY, "BY")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "XX")
        self.assert_next_token(lexer, DefaultKeyword.DESC, "DESC")
        self.assert_next_token(lexer, Assist.END, "")

    def test_next_token_for_ambiguous_group_by(self):
        lexer = Lexer("SELECT * FROM GROUP GROUP \t BY XX DESC", LexerTestCase.dictionary)
        self.assert_next_token(lexer, DefaultKeyword.SELECT, "SELECT")
        self.assert_next_token(lexer, Symbol.STAR, "*")
        self.assert_next_token(lexer, DefaultKeyword.FROM, "FROM")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "GROUP")
        self.assert_next_token(lexer, DefaultKeyword.GROUP, "GROUP")
        self.assert_next_token(lexer, DefaultKeyword.BY, "BY")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "XX")
        self.assert_next_token(lexer, DefaultKeyword.DESC, "DESC")
        self.assert_next_token(lexer, Assist.END, "")

    def assert_next_token(self, lexer, expected_token_type, expected_literals):
        lexer.next_token()
        current_token = lexer.get_current_token()
        self.assertEqual(current_token.token_type, expected_token_type)
        self.assertEqual(current_token.literals, expected_literals)

    def test_next_token_for_number(self):
        self.assert_next_token_for_number("0x1e", Literals.HEX)
        self.assert_next_token_for_number("0x-1e", Literals.HEX)
        self.assert_next_token_for_number("1243", Literals.INT)
        self.assert_next_token_for_number("-123", Literals.INT)
        self.assert_next_token_for_number("-.123", Literals.FLOAT)
        self.assert_next_token_for_number("123.0", Literals.FLOAT)
        self.assert_next_token_for_number("123e4", Literals.FLOAT)
        self.assert_next_token_for_number("123E4", Literals.FLOAT)
        self.assert_next_token_for_number("123e+4", Literals.FLOAT)
        self.assert_next_token_for_number("123E+4", Literals.FLOAT)
        self.assert_next_token_for_number("123e-4", Literals.FLOAT)
        self.assert_next_token_for_number("123E-4", Literals.FLOAT)
        self.assert_next_token_for_number(".5", Literals.FLOAT)
        self.assert_next_token_for_number("123f", Literals.FLOAT)
        self.assert_next_token_for_number("123F", Literals.FLOAT)
        self.assert_next_token_for_number(".5F", Literals.FLOAT)
        self.assert_next_token_for_number("123d", Literals.FLOAT)
        self.assert_next_token_for_number("123D", Literals.FLOAT)

    def assert_next_token_for_number(self, expected_number, expected_token_type):
        lexer = Lexer("select * from XXX_TABLE where xx={} and yy={}".format(expected_number, expected_number),
                      LexerTestCase.dictionary)
        self.assert_next_token(lexer, DefaultKeyword.SELECT, "select")
        self.assert_next_token(lexer, Symbol.STAR, "*")
        self.assert_next_token(lexer, DefaultKeyword.FROM, "from")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "XXX_TABLE")
        self.assert_next_token(lexer, DefaultKeyword.WHERE, "where")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "xx")
        self.assert_next_token(lexer, Symbol.EQ, "=")
        self.assert_next_token(lexer, expected_token_type, expected_number)
        self.assert_next_token(lexer, DefaultKeyword.AND, "and")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "yy")
        self.assert_next_token(lexer, Symbol.EQ, "=")
        self.assert_next_token(lexer, expected_token_type, expected_number)
        self.assert_next_token(lexer, Assist.END, "")

    def test_next_token_for_single_line_comment(self):
        lexer = Lexer("SELECT * FROM XXX_TABLE --x\"y`z \n WHERE XX=1 //x\"y'z", LexerTestCase.dictionary)
        self.assert_next_token(lexer, DefaultKeyword.SELECT, "SELECT")
        self.assert_next_token(lexer, Symbol.STAR, "*")
        self.assert_next_token(lexer, DefaultKeyword.FROM, "FROM")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "XXX_TABLE")
        self.assert_next_token(lexer, DefaultKeyword.WHERE, "WHERE")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "XX")
        self.assert_next_token(lexer, Symbol.EQ, "=")
        self.assert_next_token(lexer, Literals.INT, "1")
        self.assert_next_token(lexer, Assist.END, "")

    def test_next_token_for_multiple_line_comment(self):
        lexer = Lexer("SELECT * FROM XXX_TABLE /*--xyz \n WHERE XX=1 //xyz*/ WHERE YY>2 /*--xyz //xyz*/",
                      LexerTestCase.dictionary)
        self.assert_next_token(lexer, DefaultKeyword.SELECT, "SELECT")
        self.assert_next_token(lexer, Symbol.STAR, "*")
        self.assert_next_token(lexer, DefaultKeyword.FROM, "FROM")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "XXX_TABLE")
        self.assert_next_token(lexer, DefaultKeyword.WHERE, "WHERE")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "YY")
        self.assert_next_token(lexer, Symbol.GT, ">")
        self.assert_next_token(lexer, Literals.INT, "2")
        self.assert_next_token(lexer, Assist.END, "")

    def test_next_token_for_n_char(self):
        lexer = Lexer("SELECT * FROM XXX_TABLE WHERE XX=N'xx'", LexerTestCase.dictionary)
        self.assert_next_token(lexer, DefaultKeyword.SELECT, "SELECT")
        self.assert_next_token(lexer, Symbol.STAR, "*")
        self.assert_next_token(lexer, DefaultKeyword.FROM, "FROM")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "XXX_TABLE")
        self.assert_next_token(lexer, DefaultKeyword.WHERE, "WHERE")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "XX")
        self.assert_next_token(lexer, Symbol.EQ, "=")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "N")
        self.assert_next_token(lexer, Literals.CHARS, "xx")
        self.assert_next_token(lexer, Assist.END, "")

    def test_syntax_error_for_unclosed_char(self):
        lexer = Lexer("UPDATE product p SET p.title='Title's',s.description='中文' WHERE p.product_id=?",
                      LexerTestCase.dictionary)
        self.assert_next_token(lexer, DefaultKeyword.UPDATE, "UPDATE")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "product")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "p")
        self.assert_next_token(lexer, DefaultKeyword.SET, "SET")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "p")
        self.assert_next_token(lexer, Symbol.DOT, ".")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "title")
        self.assert_next_token(lexer, Symbol.EQ, "=")
        self.assert_next_token(lexer, Literals.CHARS, "Title")
        self.assert_next_token(lexer, Literals.IDENTIFIER, "s")
        self.assert_next_token(lexer, Literals.CHARS, ",s.description=")
        try:
            lexer.next_token()
        except SQLParsingException as e:
            self.assertEqual(str(e),
                             "SQL syntax error, expected token is ERROR, actual token is CHARS, literals is ',s.description='.")


class MySQLLexerTest(unittest.TestCase):
    def test_next_token_for_hint(self):
        lexer = MySQLLexer("SELECT * FROM XXX_TABLE /*! hint 1 \n xxx */ WHERE XX>1 /*!hint 2*/")
        self.assert_next_token(lexer, DefaultKeyword.SELECT, 'SELECT')
        self.assert_next_token(lexer, Symbol.STAR, '*')
        self.assert_next_token(lexer, DefaultKeyword.FROM, 'FROM')
        self.assert_next_token(lexer, Literals.IDENTIFIER, 'XXX_TABLE')
        self.assert_next_token(lexer, DefaultKeyword.WHERE, 'WHERE')
        self.assert_next_token(lexer, Literals.IDENTIFIER, 'XX')
        self.assert_next_token(lexer, Symbol.GT, '>')
        self.assert_next_token(lexer, Literals.INT, '1')
        self.assert_next_token(lexer, Assist.END, '')

    def test_next_token_for_comment(self):
        lexer = MySQLLexer("SELECT * FROM XXX_TABLE # xxx ")
        self.assert_next_token(lexer, DefaultKeyword.SELECT, 'SELECT')
        self.assert_next_token(lexer, Symbol.STAR, '*')
        self.assert_next_token(lexer, DefaultKeyword.FROM, 'FROM')
        self.assert_next_token(lexer, Literals.IDENTIFIER, 'XXX_TABLE')
        self.assert_next_token(lexer, Assist.END, '')

    def test_next_token_for_multipule_lines_comment(self):
        lexer = MySQLLexer("SELECT * FROM XXX_TABLE # comment 1 \n #comment 2 \r\n WHERE XX<=1")
        self.assert_next_token(lexer, DefaultKeyword.SELECT, 'SELECT')
        self.assert_next_token(lexer, Symbol.STAR, '*')
        self.assert_next_token(lexer, DefaultKeyword.FROM, 'FROM')
        self.assert_next_token(lexer, Literals.IDENTIFIER, 'XXX_TABLE')
        self.assert_next_token(lexer, DefaultKeyword.WHERE, 'WHERE')
        self.assert_next_token(lexer, Literals.IDENTIFIER, 'XX')
        self.assert_next_token(lexer, Symbol.LT_EQ, '<=')
        self.assert_next_token(lexer, Literals.INT, '1')
        self.assert_next_token(lexer, Assist.END, '')

    def test_next_token_for_variable(self):
        lexer = MySQLLexer("SELECT @x1:=1 FROM XXX_TABLE WHERE XX=  @@global.x1")
        self.assert_next_token(lexer, DefaultKeyword.SELECT, 'SELECT')
        self.assert_next_token(lexer, Literals.VARIABLE, '@x1')
        self.assert_next_token(lexer, Symbol.COLON_EQ, ':=')
        self.assert_next_token(lexer, Literals.INT, '1')
        self.assert_next_token(lexer, DefaultKeyword.FROM, 'FROM')
        self.assert_next_token(lexer, Literals.IDENTIFIER, 'XXX_TABLE')
        self.assert_next_token(lexer, DefaultKeyword.WHERE, 'WHERE')
        self.assert_next_token(lexer, Literals.IDENTIFIER, 'XX')
        self.assert_next_token(lexer, Symbol.EQ, '=')
        self.assert_next_token(lexer, Literals.VARIABLE, '@@global.x1')
        self.assert_next_token(lexer, Assist.END, '')

    def assert_next_token(self, lexer, expected_token_type, expected_literals):
        lexer.next_token()
        current_token = lexer.get_current_token()
        self.assertEqual(current_token.token_type, expected_token_type)
        self.assertEqual(current_token.literals, expected_literals)
