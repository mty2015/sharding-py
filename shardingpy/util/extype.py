# -*- coding: utf-8 -*-
import enum


class RangeType(enum.IntEnum):
    OPEN = 1
    CLOSED = 2


class Range:
    def __init__(self, lower, lower_type, upper, upper_type):
        pass
