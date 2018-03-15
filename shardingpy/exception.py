"""
"""

class ShardingJdbcException(Exception):
    pass


class SQLParsingException(ShardingJdbcException):
    def __init__(self, *args):
        if len(args) == 1:
            super().__init__(args[0])
        elif len(args) == 2:
            lexer, expected_token_type = args
            super().__init__("SQL syntax error, expected token is {}, actual token is {}, literals is {}.".format(expected_token_type, lexer.get_current_token().token_type, lexer.get_current_token().literals))


class SQLParsingUnsupportedException(ShardingJdbcException):
    def __init__(self, token_type):
        super().__init__("Not supported token {}".format(token_type))


class UnsupportedOperationException(ShardingJdbcException):
    pass
