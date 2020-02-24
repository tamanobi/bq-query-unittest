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
import pandas as pd
import json

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
        assert type(_name) is str and _name != "" and "\n" not in _name and " " not in _name and "," not in _name
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
        assert len(column_list) > 0
        self.column_list = self.list_to_columns(column_list)

    def list_to_columns(self, column_list):
        return [ColumnMeta(_name, _type) for _name, _type in column_list]

    def __str__(self):
        c = ', '.join([str(cl) for cl in self.column_list])
        return f"STRUCT<{c}>"

class Table:
    # pandasを使っても良いかもしれない
    _schema = None
    _rows = None
    _name = None

    def __init__(self, _filename: str, _schema: list, _name:str = ""):
        assert Path(_filename).exists()

        if Path(_filename).suffix == '.csv':
            self._rows = pd.read_csv(_filename, header=None, quoting=csv.QUOTE_ALL)
        elif Path(_filename).suffix == '.json':
            with open(_filename, "r") as f:
                records = json.load(f)
            self._rows = pd.DataFrame.from_records(records, columns=None)
        else:
            raise ValueError(f'{_filename} は未対応のファイル形式')

        self._schema = Schema(_schema)
        self._name = _name

    def rows(self):
        return self._rows

    def schema(self):
        return self._schema

    def dataframe_to_string_list(self):
        rows = []
        for columns in self._rows.itertuples():
            new_columns = []
            for col in list(columns)[1:]:
                if type(col) is str:
                    escaped_double_quotes = re.sub('"', r'\"', col)
                    new_columns += [f'"{escaped_double_quotes}"']
                else:
                    new_columns += [str(col)]
            rows += [new_columns]

        return rows

    @staticmethod
    def sql_string(rows):
        with_parens = []
        for columns in rows:
            column_string = ",".join(columns)
            with_parens += [f"({column_string})"]
        with_parens_string = ",".join(with_parens)
        return f"[{with_parens_string}]"

    def to_sql(self):
        header = str(self._schema)
        datum = Table.sql_string(self.dataframe_to_string_list())

        return "\n".join([
            f"{self._name} AS (",
            f"SELECT * FROM UNNEST(ARRAY<{header}>",
            f"{datum}",
            ")",
            ")"
        ])

class TemporaryTables:
    _tables = []
    def __init__(self, pairs: list):
        # スキーマとデータのペアをリストで受け取る
        self._tables = [Table(filename, schema, name)for filename, schema, name in pairs]

    def to_sql(self):
        table_strings = ",".join([table.to_sql() for table in self._tables])
        return f"WITH {table_strings}"
