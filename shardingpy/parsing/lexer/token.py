import enum

from shardingpy.exception import ShardingException

EOI = chr(0x1A)


class UnterminatedCharException(ShardingException):
    pass


def is_white_space(ch):
    return (ord(ch) <= 0x20 and ch != EOI) or (0xA0 >= ord(ch) > 0x7F)


def is_end_of_sql(ch):
    return ch == EOI


def is_alphabet(ch):
    return 'A' <= ch <= 'Z' or 'a' <= ch <= 'z'


def is_digital(ch):
    return '0' <= ch <= '9'


def is_hex(ch):
    return 'A' <= ch <= 'F' or 'a' <= ch <= 'f' or is_digital(ch)


def is_symbol(ch):
    return ch in "()[]{}+-*/%^=><~!?&|.:#,;"


class Token:
    def __init__(self, token_type, literals, end_position):
        self.token_type = token_type
        self.literals = literals
        self.end_position = end_position

    def __str__(self):
        return "token_type: {}-{} ; literals: {}; end_position: {}".format(self.token_type.__class__,
                                                                           self.token_type.name, self.literals,
                                                                           self.end_position)


class Tokenizer:
    MYSQL_SPECIAL_COMMENT_BEGIN_SYMBOL_LENGTH = 1
    COMMENT_BEGIN_SYMBOL_LENGTH = 2
    HINT_BEGIN_SYMBOL_LENGTH = 3
    COMMENT_AND_HINT_END_SYMBOL_LENGTH = 2
    HEX_BEGIN_SYMBOL_LENGTH = 2

    def __init__(self, sql, dictionary):
        self.sql = sql
        self.dictionary = dictionary
        self.offset = 0

    def pos(self, offset):
        self.offset = offset
        return self

    def advance(self, length):
        self.offset += length
        return self.offset

    def skip_white_space(self):
        length = 0
        while is_white_space(self._char_at(self.offset + length)):
            length += 1
        return self.advance(length)

    def skip_hint(self):
        return self.until_comment_and_hint_terminate_sign(Tokenizer.HINT_BEGIN_SYMBOL_LENGTH)

    def until_comment_and_hint_terminate_sign(self, begin_symbol_length):
        length = begin_symbol_length
        while not self._is_multiple_line_comment_end(self._char_at(self.offset + length),
                                                     self._char_at(self.offset + length + 1)):
            if is_end_of_sql(self._char_at(self.offset + length)):
                raise UnterminatedCharException("*/")
            length += 1
        return self.advance(length + Tokenizer.COMMENT_AND_HINT_END_SYMBOL_LENGTH)

    def skip_comment(self):
        ch = self._char_at(self.offset)
        next_ch = self._char_at(self.offset + 1)
        if self._is_single_line_comment_begin(ch, next_ch):
            return self.skip_single_line_comment(Tokenizer.COMMENT_BEGIN_SYMBOL_LENGTH)
        elif ch == '#':
            return self.skip_single_line_comment(Tokenizer.MYSQL_SPECIAL_COMMENT_BEGIN_SYMBOL_LENGTH)
        elif self._is_multiple_line_comment_begin(ch, next_ch):
            return self.skip_multi_line_comment()

        return self.offset

    def skip_single_line_comment(self, comment_symbol_length):
        length = comment_symbol_length
        while not is_end_of_sql(self._char_at(self.offset + length)) and self._char_at(self.offset + length) != '\n':
            length += 1
        return self.advance(length + 1)

    def skip_multi_line_comment(self):
        return self.until_comment_and_hint_terminate_sign(Tokenizer.COMMENT_BEGIN_SYMBOL_LENGTH)

    def scan_variable(self):
        length = 1
        if self._char_at(self.offset + 1) == '@':
            length += 1

        while self._is_variable_char(self._char_at(self.offset + length)):
            length += 1

        return Token(Literals.VARIABLE, self.sql[self.offset: self.offset + length], self.advance(length))

    def scan_chars(self):
        terminated_ch = self._char_at(self.offset)
        length = self._get_length_until_terminated_char(terminated_ch)
        return Token(Literals.CHARS, self.sql[self.offset + 1: self.offset + length - 1], self.advance(length))

    def scan_identifier(self):
        if self._char_at(self.offset) == '`':
            length = self._get_length_until_terminated_char('`')
            return Token(Literals.IDENTIFIER, self.sql[self.offset:self.offset + length], self.advance(length))
        length = 0
        while self._is_identifier_char(self._char_at(self.offset + length)):
            length += 1
        literals = self.sql[self.offset: self.offset + length]
        if self._is_ambiguous_identifier(literals):
            return Token(self._process_ambiguous_identifier(self.offset + length, literals), literals,
                         self.advance(length))

        return Token(self.dictionary.find_token_type_with_default(literals, Literals.IDENTIFIER), literals,
                     self.advance(length))

    def scan_hex_decimal(self):
        length = Tokenizer.HEX_BEGIN_SYMBOL_LENGTH
        if self._char_at(self.offset + length) == '-':
            length += 1
        while is_hex(self._char_at(self.offset + length)):
            length += 1
        return Token(Literals.HEX, self.sql[self.offset: self.offset + length], self.advance(length))

    def scan_number(self):
        length = 0

        if self._char_at(self.offset + length) == '-':
            length += 1

        length += self._get_digital_length(self.offset + length)
        is_float = False

        if self._char_at(self.offset + length) == '.':
            is_float = True
            length += 1
            length += self._get_digital_length(self.offset + length)

        if self._is_scientifc_notation(self.offset + length):
            is_float = True
            length += 1
            if self._char_at(self.offset + length) in "+-":
                length += 1
            length += self._get_digital_length(self.offset + length)

        if self._is_binary_number(self.offset + length):
            is_float = True
            length += 1

        return Token(Literals.FLOAT if is_float else Literals.INT, self.sql[self.offset:self.offset + length],
                     self.advance(length))

    def scan_symbol(self):
        symbol_len = length = 0
        symbol = None
        while is_symbol(self._char_at(self.offset + length)):
            try:
                symbol = Symbol(self.sql[self.offset:self.offset + length + 1])
                symbol_len = length + 1
            except ValueError:
                pass
            length += 1

        return Token(symbol, self.sql[self.offset:self.offset + symbol_len], self.advance(symbol_len))

    def _is_multiple_line_comment_end(self, ch, next_ch):
        return '*' == ch and '/' == next_ch

    def _is_single_line_comment_begin(self, ch, next_ch):
        return (ch == '/' and next_ch == '/') or (ch == '-' or next_ch == '-')

    def _is_multiple_line_comment_begin(self, ch, next_ch):
        return ch == '/' and next_ch == '*'

    def _is_variable_char(self, ch):
        return self._is_identifier_char(ch) or ch == '.'

    def _is_identifier_char(self, ch):
        return is_alphabet(ch) or is_digital(ch) or ch in "_$#"

    def _get_length_until_terminated_char(self, terminated_char):
        length = 1
        while terminated_char != self._char_at(self.offset + length) or self._has_escape_char(terminated_char,
                                                                                              self.offset + length):
            if self.offset + length >= len(self.sql):
                raise UnterminatedCharException(terminated_char)
            if self._has_escape_char(terminated_char, self.offset + length):
                length += 1
            length += 1
        return length + 1

    def _has_escape_char(self, char_identifier, offset):
        return self._char_at(self.offset) == char_identifier and self._char_at(self.offset + 1) == char_identifier

    def _is_ambiguous_identifier(self, literals):
        return DefaultKeyword.ORDER.name == literals.upper() or DefaultKeyword.GROUP.name == literals.upper()

    def _process_ambiguous_identifier(self, offset, literials):
        i = 0
        while is_white_space(self._char_at(offset + i)):
            i += 1
        if DefaultKeyword.BY.name == self._char_at(offset + i).upper() + self._char_at(offset + i + 1).upper():
            return self.dictionary.find_token_type(literials)

        return Literals.IDENTIFIER

    def _get_digital_length(self, offset):
        length = 0
        while is_digital(self._char_at(offset + length)):
            length += 1
        return length

    def _is_scientifc_notation(self, offset):
        return self._char_at(offset) in "Ee"

    def _is_binary_number(self, offset):
        return self._char_at(offset) in "fFdD"

    def _char_at(self, index):
        return self.sql[index] if index < len(self.sql) else EOI


class Dictionary:
    def __init__(self, *dialect_keywords):
        self.keywords = [DefaultKeyword, *dialect_keywords]

    def find_token_type(self, literals):
        for kw in self.keywords:
            try:
                return kw[literals.upper()]
            except KeyError:
                continue
        raise ValueError("{} is not keyword literals".format(literals))

    def find_token_type_with_default(self, literals, default_token_type):
        try:
            return self.find_token_type(literals)
        except ValueError:
            return default_token_type


class Symbol(enum.Enum):
    LEFT_PAREN = "("
    RIGHT_PAREN = ")"
    LEFT_BRACE = "{"
    RIGHT_BRACE = "}"
    LEFT_BRACKET = "["
    RIGHT_BRACKET = "]"
    SEMI = ";"
    COMMA = ","
    DOT = "."
    DOUBLE_DOT = ".."
    PLUS = "+"
    SUB = "-"
    STAR = "*"
    SLASH = "/"
    QUESTION = "?"
    EQ = "="
    GT = ">"
    LT = "<"
    BANG = "!"
    TILDE = "~"
    CARET = "^"
    PERCENT = "%"
    COLON = ":"
    DOUBLE_COLON = "::"
    COLON_EQ = ":="
    LT_EQ = "<="
    GT_EQ = ">="
    LT_EQ_GT = "<=>"
    LT_GT = "<>"
    BANG_EQ = "!="
    BANG_GT = "!>"
    BANG_LT = "!<"
    AMP = "&"
    BAR = "|"
    DOUBLE_AMP = "&&"
    DOUBLE_BAR = "||"
    DOUBLE_LT = "<<"
    DOUBLE_GT = ">>"
    AT = "@"
    POUND = "#"

    @classmethod
    def operators(cls):
        return [Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.EQ, Symbol.GT, Symbol.LT, Symbol.CARET,
                Symbol.PERCENT, Symbol.LT_EQ, Symbol.GT_EQ, Symbol.LT_EQ_GT, Symbol.LT_GT, Symbol.BANG_EQ,
                Symbol.BANG_GT, Symbol.BANG_LT, Symbol.AMP, Symbol.BAR, Symbol.DOUBLE_AMP, Symbol.DOUBLE_BAR,
                Symbol.DOUBLE_LT, Symbol.DOUBLE_GT]


class Literals(enum.IntEnum):
    INT = 1
    FLOAT = 2
    HEX = 3
    CHARS = 4
    IDENTIFIER = 5
    VARIABLE = 6


class DefaultKeyword(enum.IntEnum):
    # Common
    SCHEMA = 1
    DATABASE = 2
    TABLE = 3
    COLUMN = 4
    VIEW = 5
    INDEX = 6
    TRIGGER = 7
    PROCEDURE = 8
    TABLESPACE = 9
    FUNCTION = 10
    SEQUENCE = 11
    CURSOR = 12
    FROM = 13
    TO = 14
    OF = 15
    IF = 16
    ON = 17
    FOR = 18
    WHILE = 19
    DO = 20
    NO = 21
    BY = 22
    WITH = 23
    WITHOUT = 24
    TRUE = 25
    FALSE = 26
    TEMPORARY = 27
    TEMP = 28
    COMMENT = 29
    # Create
    CREATE = 100
    REPLACE = 101
    BEFORE = 102
    AFTER = 103
    INSTEAD = 104
    EACH = 105
    ROW = 106
    STATEMENT = 107
    EXECUTE = 108
    BITMAP = 109
    NOSORT = 110
    REVERSE = 111
    COMPILE = 112
    # Alter
    ALTER = 200
    ADD = 201
    MODIFY = 202
    RENAME = 203
    ENABLE = 204
    DISABLE = 205
    VALIDATE = 206
    USER = 207
    IDENTIFIED = 208
    # Truncate
    TRUNCATE = 300
    # Drop
    DROP = 400
    CASCADE = 401
    # Insert
    INSERT = 500
    INTO = 501
    VALUES = 502
    # Update
    UPDATE = 503
    SET = 504
    # Delete
    DELETE = 505
    # Select
    SELECT = 506
    DISTINCT = 507
    AS = 508
    CASE = 509
    WHEN = 510
    ELSE = 511
    THEN = 512
    END = 513
    LEFT = 514
    RIGHT = 515
    FULL = 516
    INNER = 517
    OUTER = 518
    CROSS = 519
    JOIN = 520
    USE = 521
    USING = 522
    NATURAL = 523
    WHERE = 524
    ORDER = 525
    ASC = 526
    DESC = 527
    GROUP = 528
    HAVING = 529
    UNION = 530
    # Other Command
    DECLARE = 600
    GRANT = 601
    FETCH = 602
    REVOKE = 603
    CLOSE = 604
    # Others
    CAST = 605
    NEW = 606
    ESCAPE = 607
    LOCK = 608
    SOME = 609
    LEAVE = 610
    ITERATE = 611
    REPEAT = 612
    UNTIL = 613
    OPEN = 614
    OUT = 615
    INOUT = 616
    OVER = 617
    ADVISE = 618
    SIBLINGS = 619
    LOOP = 620
    EXPLAIN = 621
    DEFAULT = 622
    EXCEPT = 623
    INTERSECT = 624
    MINUS = 625
    PASSWORD = 626
    LOCAL = 627
    GLOBAL = 628
    STORAGE = 629
    DATA = 630
    COALESCE = 631
    # Types
    CHAR = 700
    CHARACTER = 701
    VARYING = 702
    VARCHAR = 703
    VARCHAR2 = 704
    INTEGER = 705
    INT = 706
    SMALLINT = 707
    DECIMAL = 708
    DEC = 709
    NUMERIC = 710
    FLOAT = 711
    REAL = 712
    DOUBLE = 713
    PRECISION = 714
    DATE = 715
    TIME = 716
    INTERVAL = 717
    BOOLEAN = 718
    BLOB = 719
    # Conditionals
    AND = 720
    OR = 721
    XOR = 722
    IS = 723
    NOT = 724
    NULL = 725
    IN = 726
    BETWEEN = 727
    LIKE = 728
    ANY = 729
    ALL = 730
    EXISTS = 731
    # Functions
    AVG = 800
    MAX = 801
    MIN = 802
    SUM = 803
    COUNT = 804
    GREATEST = 805
    LEAST = 806
    ROUND = 807
    TRUNC = 808
    POSITION = 809
    EXTRACT = 810
    LENGTH = 811
    CHAR_LENGTH = 812
    SUBSTRING = 813
    SUBSTR = 814
    INSTR = 815
    INITCAP = 816
    UPPER = 817
    LOWER = 818
    TRIM = 819
    LTRIM = 820
    RTRIM = 821
    BOTH = 822
    LEADING = 823
    TRAILING = 824
    TRANSLATE = 825
    CONVERT = 826
    LPAD = 827
    RPAD = 828
    DECODE = 829
    NVL = 830
    # Constraints
    CONSTRAINT = 900
    UNIQUE = 901
    PRIMARY = 902
    FOREIGN = 903
    KEY = 904
    CHECK = 905
    REFERENCE = 906


class Assist(enum.IntEnum):
    END = 1
    ERROR = 2
