import re


def get_exactly_value(value):
    return re.sub(r'[\[\]`\'"]', '', value) if value else None
