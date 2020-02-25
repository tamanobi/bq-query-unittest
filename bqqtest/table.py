import csv
import re
from pathlib import Path
import pandas as pd
import json
from google.cloud import bigquery
import random, string


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


class Schema:
    def __init__(self, column_list):
        assert len(column_list) > 0
        self.column_list = self.list_to_columns(column_list)

    def list_to_columns(self, column_list):
        return [ColumnMeta(_name, _type) for _name, _type in column_list]

    def __str__(self):
        c = ", ".join([str(cl) for cl in self.column_list])
        return f"STRUCT<{c}>"

    def names(self):
        return [col.name() for col in self.column_list]


class Table:
    # pandasを使っても良いかもしれない
    _schema = None
    _rows = None
    _name = None

    def __init__(self, _filename_or_list, _schema: list, _name: str = ""):
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
        rows = []
        for columns in self._rows.itertuples():
            new_columns = []
            for col in list(columns)[1:]:
                if type(col) is str:
                    escaped_double_quotes = re.sub('"', r"\"", col)
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

        return "\n".join(
            [
                f"{self._name} AS (",
                f"SELECT * FROM UNNEST(ARRAY<{header}>",
                f"{datum}",
                ")",
                ")",
            ]
        )


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

    def run(self):
        """テストを実際に走らせる

        Returns:
            tuple: 成功か失敗を示すBoolと文字列
        """
        query = self.build()
        query_parameters = self._query.query_parameters()

        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        result = list(self._client.query(query, job_config=job_config))
        return (len(result) == 0, result)


class QueryTest:
    _qlt = None

    def __init__(self, _client, _expected: dict, _tables: list, _query: dict):
        expected = Table(_expected["datum"], _expected["schema"], "EXPECTED")

        table_map = {table["name"]: randomname(16) for table in _tables}
        tables = [
            Table(table["datum"], table["schema"], table_map[table["name"]])
            for table in _tables
        ]
        query = Query("ACTUAL", _query["query"], _query["params"], table_map)
        self._qlt = QueryLogicTest(_client, expected, tables, query)

    def run(self):
        return self._qlt.run()

