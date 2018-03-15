import enum
from shardingpy.parsing.lexer import Lexer
from shardingpy.parsing.token import Dictionary


class MySQLLexer(Lexer):
    dictionary = Dictionary(MySQLKeyword)

    def __init__(self, sql):
        super().__init__(sql, MySQLLexer.dictionary)

    def is_hint_begin(self):
        return self.get_current_char(0) == '/' and self.get_current_char(1) == '*' and self.get_current_char(2) == '!'

    def is_comment_begin(self):
        return self.get_current_char(0) == '#' or super().is_comment_begin()

    def is_variable_begin(self):
        return self.get_current_char(0) == '@'
    

class MySQLKeyword(enum.IntEnum):
    SHOW = 1
    DUAL = 2
    LIMIT = 3
    OFFSET = 4
    VALUE = 5
    BEGIN = 6
    FORCE = 7
    PARTITION = 8
    DISTINCTROW = 9
    KILL = 10
    QUICK = 11
    BINARY = 12
    CACHE = 13
    SQL_CACHE = 14
    SQL_NO_CACHE = 15
    SQL_SMALL_RESULT = 16
    SQL_BIG_RESULT = 17
    SQL_BUFFER_RESULT = 18
    SQL_CALC_FOUND_ROWS = 19
    LOW_PRIORITY = 20
    HIGH_PRIORITY = 21
    OPTIMIZE = 22
    ANALYZE = 23
    IGNORE = 24
    CHANGE = 25
    FIRST = 26
    SPATIAL = 27
    ALGORITHM = 28
    COLLATE = 29
    DISCARD = 30
    IMPORT = 31
    VALIDATION = 32
    REORGANIZE = 33
    EXCHANGE = 34
    REBUILD = 35
    REPAIR = 36
    REMOVE = 37
    UPGRADE = 38
    KEY_BLOCK_SIZE = 39
    AUTO_INCREMENT = 40
    AVG_ROW_LENGTH = 41
    CHECKSUM = 42
    COMPRESSION = 43
    CONNECTION = 44
    DIRECTORY = 45
    DELAY_KEY_WRITE = 46
    ENCRYPTION = 47
    ENGINE = 48
    INSERT_METHOD = 49
    MAX_ROWS = 50
    MIN_ROWS = 51
    PACK_KEYS = 52
    ROW_FORMAT = 53
    DYNAMIC = 54
    FIXED = 55
    COMPRESSED = 56
    REDUNDANT = 57
    COMPACT = 58
    STATS_AUTO_RECALC = 59
    STATS_PERSISTENT = 60
    STATS_SAMPLE_PAGES = 61
    DISK = 62
    MEMORY = 63
    ROLLUP = 64
    RESTRICT = 65
    STRAIGHT_JOIN = 66
    REGEXP= 67
