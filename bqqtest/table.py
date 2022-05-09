import csv
import json
import random
import re
import string
from pathlib import Path

import pandas as pd
import regex
from google.cloud import bigquery

from .util import get_query_from_with_clause


def randomname(n):
    """ランダムな文字列を返す

    Args:
        n (int): 文字列長

    Returns:
        (str): ランダムな文字列(n文字)
    """
    return "".join(random.choices(string.ascii_letters, k=n))


class ColumnMeta:
    usable_primitive_types = [
        "INT64",
        "NUMERIC",
        "FLOAT64",
        "BOOL",
        "STRING",
        "BYTES",
        "DATE",
        "DATETIME",
        "GEOGRAPHY",
        "TIME",
        "TIMESTAMP",
    ]

    def __init__(self, _name, _type):
        assert (
            type(_name) is str
            and _name != ""
            and "\n" not in _name
            and " " not in _name
            and "," not in _name
        )
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

    def name(self):
        return self._name

    def typ(self):  # avoid reserved word
        return self._type


class Schema:
    def __init__(self, columns: list):
        assert type(columns) is list and columns
        self.column_list = self.list_to_columns(columns)

    def list_to_columns(self, columns: list):
        return [ColumnMeta(column["name"], column["type"]) for column in columns]

    def __str__(self):
        if len(self.column_list) == 1:
            return ""

        c = ", ".join([str(cl) for cl in self.column_list])
        return f"STRUCT<{c}>"

    def names(self):
        return [col.name() for col in self.column_list]

    def types(self):
        return [col.typ().lower() for col in self.column_list]

class Table:
    # pandasを使っても良いかもしれない
    _schema = None
    _rows = None
    _name = None

    def __init__(self, _filename_or_list, _schema: list, _name: str = ""):
        assert type(_filename_or_list) is str or type(_filename_or_list) is list
        assert type(_schema) is list and _schema

        self._schema = Schema(_schema)
        self._name = _name

        header = self._schema.names()
        if type(_filename_or_list) is str:
            filename = _filename_or_list
            assert Path(filename).exists()

            if Path(filename).suffix == ".csv":
                self._rows = pd.read_csv(
                    filename, header=None, names=header, quoting=csv.QUOTE_ALL
                )
            elif Path(filename).suffix == ".json":
                with open(filename, "r") as f:
                    records = json.load(f)
                self._rows = pd.DataFrame.from_records(records, columns=header)
            else:
                raise ValueError(f"{filename} は未対応のファイル形式")
        elif type(_filename_or_list) is list:
            records = _filename_or_list
            self._rows = pd.DataFrame.from_records(records, columns=header)
        else:
            raise ValueError("ファイルパスか、listのみに対応しています")

    def dataframe_to_string_list(self):
        df_types = dict(zip(self._schema.names(), self._schema.types()))
        rows = []
        for columns in self._rows.itertuples():
            cols = columns._asdict()
            new_columns = []
            for key in cols.keys():
                if key == 'Index':
                    continue
                if cols[key] is None:
                    new_columns += ['null']
                elif repr(cols[key]) == 'nan':
                    new_columns += ['null']
                elif df_types[key] == 'int64':
                    new_columns += [str(int(cols[key]))]
                elif type(cols[key]) is str:
                    escaped_double_quotes = re.sub('"', r"\"", str(cols[key]))
                    new_columns += [f'"{escaped_double_quotes}"']
                else:
                    new_columns += [str(cols[key])]
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

        if header != "":
            header = f"<{header}>"

        return "\n".join(
            [
                f"{self._name} AS (",
                f"SELECT * FROM UNNEST(ARRAY{header}",
                f"{datum}",
                ")",
                ")",
            ]
        )


class NamedQueryTable:
    _name = ""
    _query = ""

    def __init__(self, name: str, query: str):
        assert isinstance(name, str)
        assert isinstance(query, str)

        self._name = name
        self._query = query

    def to_sql(self):
        return "\n".join([f"{self._name} AS (", f"{self._query}", ")"])


class TemporaryTables:
    _tables = []

    def __init__(self, pairs: list):
        # スキーマとデータのペアをリストで受け取る
        self._tables = [
            Table(filename, schema, name) for filename, schema, name in pairs
        ]

    def to_sql(self):
        table_strings = ",".join([table.to_sql() for table in self._tables])
        return f"WITH {table_strings}"


class Query:
    _name = ""
    _query = ""
    _query_parameters = []
    _table_map = {}

    def __init__(self, name: str, query: str, query_parameters: list, table_map: dict):
        assert type(name) is str and name != "" and " " not in name and "," not in name
        assert type(query) is str and query != ""
        assert type(query_parameters) is list
        assert type(table_map) is dict
        if re.match(r"CREATE\s(TABLE|OR)", query, flags=re.IGNORECASE):
            raise NotImplementedError("CREATE ステートメントは副作用が生じるため未対応")

        self._name = name
        self._query = query
        self._query_parameters = query_parameters
        self._table_map = table_map

    def to_sql(self):
        query = self._query
        for before, after in self._table_map.items():
            query = re.sub(before, after, query)
        return f"{self._name} AS ({query})"

    def query_parameters(self):
        return self._query_parameters


class QueryLogicTest:
    """クエリロジックのテスト"""

    _client = None
    _tables = []
    _expected = None
    _query = None

    def __init__(
        self, client, expected_table: "Table", input_tables: list, query: "Query"
    ):
        self._client = client
        self._expected = expected_table
        self._tables = input_tables
        self._query = query

    def build(self):
        diff = Query(
            "diff",
            """
SELECT "+" AS mark , * FROM (SELECT *, ROW_NUMBER() OVER() AS n FROM ACTUAL EXCEPT DISTINCT SELECT *, ROW_NUMBER() OVER() AS n FROM EXPECTED) UNION ALL
SELECT "-" AS mark , * FROM (SELECT *, ROW_NUMBER() OVER() AS n FROM EXPECTED EXCEPT DISTINCT SELECT *, ROW_NUMBER() OVER() AS n FROM ACTUAL) ORDER BY n ASC
""",
            [],
            {},
        )
        tables = self._tables + [self._expected] + [self._query] + [diff]
        with_clause = ",".join([table.to_sql() for table in tables])
        return f"WITH {with_clause} SELECT * FROM diff"

    def is_total_bytes_processed_zero(self):
        """ドライランによってデータ走査量がゼロかどうか判定する

        セーフティネット

        Returns:
            (bool): データ走査量がゼロならTrue。それ以外はFalse
        """
        query_job = self._client.query(
            self.build(),
            job_config=bigquery.QueryJobConfig(
                query_parameters=self._query.query_parameters(),
                dry_run=False,
                use_query_cache=False,
            ),
        )
        query_job.result()
        return query_job.total_bytes_processed == 0

    def run(self):
        """テストを実際に走らせる

        Returns:
            tuple: 成功か失敗を示すBoolと文字列
        """
        assert (
            self.is_total_bytes_processed_zero()
        ), "クエリのデータ走査量がゼロではありません。クエリを再確認してください"

        query = self.build()
        query_parameters = self._query.query_parameters()
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters, dry_run=False, use_query_cache=False
        )
        query_job = self._client.query(query, job_config=job_config)

        result = query_job.result()
        return (result.total_rows == 0, [r for r in result])


class QueryTest:
    _qlt = None

    def __init__(self, _client, _expected: dict, _tables: dict, _query: dict):
        expected = Table(_expected["datum"], _expected["schema"], "EXPECTED")

        table_map = {name: randomname(16) for name, table in _tables.items()}
        tables = [
            Table(table["datum"], table["schema"], table_map[name])
            for name, table in _tables.items()
        ]

        # FIXME: WITH句の解析を正規表現で強引に行っているため保守性が低い
        tables = tables + [
            NamedQueryTable(name, query)
            for name, query in get_query_from_with_clause(_query["query"])
        ]

        query = regex.sub(
            r"WITH\s+(?<name>\w+)\s+AS\s+(?<query>\((?:[^\(\)]+|(?&query))*\))",
            "",
            _query["query"],
        )
        query = Query("ACTUAL", query, _query["params"], table_map)
        self._qlt = QueryLogicTest(_client, expected, tables, query)

    def build(self):
        return self._qlt.build()

    def run(self):
        return self._qlt.run()
