# -*- coding: utf-8 -*-
import enum


class RangeType(enum.IntEnum):
    OPEN = 1
    CLOSED = 2


class Range:
    def __init__(self, lower, lower_type, upper, upper_type):
        self.lower = lower
        self.lower_type = lower_type
        self.upper = upper
        self.upper_type = upper_type

    def intersection(self, other):
        # TODO 这里只考虑了closed type
        if not other:
            return None
        assert isinstance(other, Range)
        if other.lower > self.upper or other.upper < self.lower:
            return None
        return Range(max(self.lower, other.lower), RangeType.CLOSED, min(self.upper, other.upper), RangeType.CLOSED)

    def contains(self, value):
        result = value > self.lower if self.lower_type == RangeType.OPEN else value >= self.lower
        result = result and (value < self.upper if self.upper_type == RangeType.OPEN else value <= self.upper)
        return result
