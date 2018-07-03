"""
"""


class ShardingException(Exception):
    pass


class SQLParsingException(ShardingException):
    TOKEN_ERROR_MESSAGE = "SQL syntax error, token is '{}', literals is '{}'."
    UNMATCH_MESSAGE = "SQL syntax error, expected token is {}, actual token is {}, literals is '{}'."

    def __init__(self, *args):
        if len(args) == 1:
            if isinstance(args[0], str):
                super().__init__(args[0])
            else:
                super().__init__(self.TOKEN_ERROR_MESSAGE.format(args[0].get_current_token().token_type,
                                                                 args[0].get_current_token().literals))
        elif len(args) == 2:
            _lexer, expected_token_type = args
            super().__init__(
                self.UNMATCH_MESSAGE.format(expected_token_type.name, _lexer.get_current_token().token_type.name,
                                            _lexer.get_current_token().literals))


class SQLParsingUnsupportedException(ShardingException):
    def __init__(self, token_type):
        super().__init__("Not supported token {}".format(token_type))


class UnsupportedOperationException(ShardingException):
    pass


class ShardingConfigurationException(ShardingException):
    pass
