# -*- coding: utf-8 -*-


class Table:
    def __init__(self, name, alias):
        self.name = name
        self.alias = alias

    def __eq__(self, other):
        return other and isinstance(other, Table) and self.__dict__ == other.__dict__


class Tables:
    def __init__(self):
        # collection contains Table
        self.tables = []

    def add(self, table):
        assert isinstance(table, Table)
        self.tables.append(table)

    def is_empty(self):
        return len(self.tables) == 0

    def is_single_table(self):
        return len(self.tables) == 1

    def get_single_table_name(self):
        assert not self.is_empty()
        return self.tables[0].name

    def get_table_names(self):
        return set([t.name for t in self.tables])

    def find(self, table_name_or_alias):
        table = self._find_table_from_name(table_name_or_alias)
        return table if table else self._find_table_from_alias(table_name_or_alias)

    def _find_table_from_name(self, name):
        for t in self.tables:
            if t.name == name:
                return t

    def _find_table_from_alias(self, alias):
        for t in self.tables:
            if t.alias and t.alias == alias:
                return t
