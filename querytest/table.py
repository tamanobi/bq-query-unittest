# unittest.run() をすると
# 要求：クエリのテストがしたい
# 用意するもの
# * インプット（複数の可能性あり）
# * クエリ
# * アウトプット
#
# クエリの対象となる差し替えができないと困難

import csv
import re
from pathlib import Path

class ColumnMeta:
    usable_primitive_types = [
        "INT64"
        , "NUMERIC"
        , "FLOAT64"
        , "BOOL"
        , "STRING"
        , "BYTES"
        , "DATE"
        , "DATETIME"
        , "GEOGRAPHY"
        , "TIME"
        , "TIMESTAMP"
    ]
    def __init__(self, _name, _type):
        assert type(_name) is str and _name != "" and "\n" not in _name and " " not in _name
        assert type(_type) is str and _type != "" and self.is_usable_type(_type)

        self._name = _name
        self._type = _type

    def is_usable_type(self, t: str):
        """BigQueryで使える型かどうか？

        Args:
            t (str): 型名

        Returns:
            bool: Trueなら使える型、Falseなら使えない型

        Note:
            ARRAY<ARRAY<STRING>> を許してしまうバグがある。このような記述は本来できない。
            see: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types?hl=ja#examples_2
        """
        if t in self.usable_primitive_types:
            return True

        if re.match(r"^(ARRAY|STRUCT)<.*>$", t):
            new_t = re.sub(r"^(ARRAY|STRUCT)<", "", t)
            new_t = re.sub(r">$", "", new_t)
            return self.is_usable_type(new_t)

        return False

    def __str__(self):
        return f"{self._name} {self._type}"

class Schema:
    def __init__(self, column_list):
        self.column_list = self.list_to_columns(column_list)

    def list_to_columns(self, column_list):
        return [ColumnMeta(_name, _type) for _name, _type in column_list]

class Table:
    # pandasを使っても良いかもしれない
    _schema = None
    _rows = None

    def __init__(self, _filename: str, _schema: list):
        assert Path(_filename).exists()

        with open(_filename, newline='') as csvfile:
            self._rows = csv.reader(csvfile, delimiter=',')

        self.schema = Schema(_schema)

        assert self.rows()

    def rows(self):
        return self._rows

    def schema():
        return self._schema



def add(a, b):
    return a + b

def main():
    pass
