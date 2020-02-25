from .table import (
    Table,
    ColumnMeta,
    Schema,
    TemporaryTables,
    Query,
    QueryLogicTest,
    QueryTest,
)
from pathlib import Path
import os
import pytest


def is_githubactions():
    return os.environ.get("GITHUB_RUN_ID") is not None


class TestTable:
    def test_table(self):
        p = Path(__file__).parent / "testdata/test1.csv"
        schema = [
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "INT64", "mode": "NULLABLE"},
        ]

        assert Table(str(p), schema)

    def test_to_sqlを呼び出すとデータから一時テーブルを作成できる(self):
        p = Path(__file__).parent / "testdata/test1.csv"
        schema = [
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "INT64", "mode": "NULLABLE"},
        ]
        t = Table(str(p), schema, "TEST_DATA")
        w = t.to_sql()
        assert (
            w
            == """TEST_DATA AS (
SELECT * FROM UNNEST(ARRAY<STRUCT<name STRING, category STRING, value INT64>>
[("abc","bdc",200),("ほげほげ","ふがふが",300000)]
)
)"""
        )

    def test_dataframe_to_string_list(self):
        p = Path(__file__).parent / "testdata/test2.json"
        schema = [
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "INT64", "mode": "NULLABLE"},
        ]
        t = Table(str(p), schema, "TEST_DATA",)

        assert t.dataframe_to_string_list() == [
            ['"abc"', '"bcd"', "300"],
            ['"ddd"', '"ccc"', "400"],
            [r'"\"xxx\""', '"yyy"', "123"],
            [r'"\"xxx\""', r'"[\"y\",\"y\",\"y\"]"', "123"],
        ]

    def test_sql_stringによってリストからsqlのレコードが生成できる(self):
        input_list = [
            ['"abc"', '"bcd"', "300"],
            ['"ddd"', '"ccc"', "400"],
            [r'"\"xxx\""', '"yyy"', "123"],
        ]
        assert (
            Table.sql_string(input_list)
            == r'[("abc","bcd",300),("ddd","ccc",400),("\"xxx\"","yyy",123)]'
        )

    def test_listからTableを生成できる(self):
        schema = [
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "INT64", "mode": "NULLABLE"},
        ]

        t = Table([["田中", 78], ["小林", 80]], schema, "TABLE1")
        assert (
            t.to_sql()
            == """TABLE1 AS (
SELECT * FROM UNNEST(ARRAY<STRUCT<name STRING, value INT64>>
[("田中",78),("小林",80)]
)
)"""
        )


class TestColumnMeta:
    def test_STRINGは使えるタイプ(self):
        assert ColumnMeta("name", "STRING")

    def test_ARRAYは使えるタイプ(self):
        assert ColumnMeta("name", "ARRAY<STRING>")

    def test_ARRAYにはカッコが必要(self):
        with pytest.raises(AssertionError):
            ColumnMeta("name", "ARRAY")

    def test_strで文字列に変更できる(self):
        assert str(ColumnMeta("name", "STRING")) == "name STRING"

    def test_nameに改行や空白文字カンマは入れられない(self):
        with pytest.raises(AssertionError):
            assert ColumnMeta("\n", "STRING")
        with pytest.raises(AssertionError):
            assert ColumnMeta("a b", "STRING")
        with pytest.raises(AssertionError):
            assert ColumnMeta(",", "STRING")

    def test_使えないタイプを入れるとAssertionError(self):
        with pytest.raises(AssertionError):
            ColumnMeta("name", "INTEGER")

    def test_空文字列はAssertionError(self):
        with pytest.raises(AssertionError):
            ColumnMeta("", "ARRAY<STRING>")

    @pytest.mark.skip(reason="ARRAYの直下にARRAYは入れられないが、バグっている")
    def test_ARRAYの直下にARRAYは入れられない(self):
        with pytest.raises(AssertionError):
            ColumnMeta("name", "ARRAY<ARRAY<STRING>>")


class TestSchema:
    def test_スキーマは空配列から生成しようとするとAssertionErrorになる(self):
        with pytest.raises(AssertionError):
            assert Schema([])

    def test_スキーマはstrでSTRUCTに変換できる(self):
        schema = [
            {"name": "time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "event", "type": "STRING", "mode": "NULLABLE"},
            {"name": "id", "type": "INT64", "mode": "NULLABLE"},
        ]

        s = Schema(schema)
        assert str(s) == "STRUCT<time TIMESTAMP, event STRING, id INT64>"


class TestTemporalTable:
    def test_一時テーブルのインスタンスが作成できる(self):
        pairs = [
            [
                str(Path(__file__).parent / "testdata/test2.json"),
                [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "TEST_DATA1",
            ],
            [
                str(Path(__file__).parent / "testdata/test1.csv"),
                [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "TEST_DATA2",
            ],
        ]

        assert TemporaryTables(pairs)

    def test_一時テーブルのインスタンスからSQLを生成できる(self):
        pairs = [
            [
                str(Path(__file__).parent / "testdata/test2.json"),
                [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "TEST_DATA1",
            ],
            [
                str(Path(__file__).parent / "testdata/test1.csv"),
                [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "TEST_DATA2",
            ],
        ]

        assert (
            TemporaryTables(pairs).to_sql()
            == r"""WITH TEST_DATA1 AS (
SELECT * FROM UNNEST(ARRAY<STRUCT<name STRING, category STRING, value INT64>>
[("abc","bcd",300),("ddd","ccc",400),("\"xxx\"","yyy",123),("\"xxx\"","[\"y\",\"y\",\"y\"]",123)]
)
),TEST_DATA2 AS (
SELECT * FROM UNNEST(ARRAY<STRUCT<name STRING, category STRING, value INT64>>
[("abc","bdc",200),("ほげほげ","ふがふが",300000)]
)
)"""
        )


class TestQueryLogicTest:
    @pytest.mark.skipif(is_githubactions(), reason="GitHub Actions")
    def test_BigQueryのクエリを生成できる(self):
        from google.cloud import bigquery

        client = bigquery.Client()

        expected = Table(
            str(Path(__file__).parent / "testdata/test3.json"),
            [
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                {"name": "value", "type": "INT64", "mode": "NULLABLE"},
            ],
            "EXPECTED",
        )
        input_tables = [
            Table(
                str(Path(__file__).parent / "testdata/test3.json"),
                [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "INPUT_DATA",
            )
        ]
        query = Query("ACTUAL", """SELECT * FROM abc""", [], {"abc": "INPUT_DATA"})

        qlt = QueryLogicTest(client, expected, input_tables, query)
        assert (
            qlt.build()
            == """WITH INPUT_DATA AS (
SELECT * FROM UNNEST(ARRAY<STRUCT<name STRING, category STRING, value INT64>>
[("abc","bcd",300),("ddd","ccc",400)]
)
),EXPECTED AS (
SELECT * FROM UNNEST(ARRAY<STRUCT<name STRING, category STRING, value INT64>>
[("abc","bcd",300),("ddd","ccc",400)]
)
),ACTUAL AS (SELECT * FROM INPUT_DATA),diff AS (
SELECT "+" AS mark , * FROM (SELECT *, ROW_NUMBER() OVER() AS n FROM ACTUAL EXCEPT DISTINCT SELECT *, ROW_NUMBER() OVER() AS n FROM EXPECTED) UNION ALL
SELECT "-" AS mark , * FROM (SELECT *, ROW_NUMBER() OVER() AS n FROM EXPECTED EXCEPT DISTINCT SELECT *, ROW_NUMBER() OVER() AS n FROM ACTUAL) ORDER BY n ASC
) SELECT * FROM diff"""
        )

    @pytest.mark.skipif(is_githubactions(), reason="GitHub Actions")
    def test_差分が見つからないクエリの場合レコードが空で返ってくる(self):
        from google.cloud import bigquery

        client = bigquery.Client()

        expected = Table(
            str(Path(__file__).parent / "testdata/test3.json"),
            [
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                {"name": "value", "type": "INT64", "mode": "NULLABLE"},
            ],
            "EXPECTED",
        )
        input_tables = [
            Table(
                str(Path(__file__).parent / "testdata/test3.json"),
                [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "INPUT_DATA",
            )
        ]
        query = Query("ACTUAL", """SELECT * FROM abc""", [], {"abc": "INPUT_DATA"})

        qlt = QueryLogicTest(client, expected, input_tables, query)
        success, _ = qlt.run()
        assert success

    @pytest.mark.skipif(is_githubactions(), reason="GitHub Actions")
    def test_期待しているテーブル結果の量よりも大きな結果になった場合マークがプラスになる差分が出る(self):
        from google.cloud import bigquery

        client = bigquery.Client()

        expected = Table(
            str(Path(__file__).parent / "testdata/test4.json"),
            [
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                {"name": "value", "type": "INT64", "mode": "NULLABLE"},
            ],
            "EXPECTED",
        )
        input_tables = [
            Table(
                str(Path(__file__).parent / "testdata/test3.json"),
                [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "INPUT_DATA",
            )
        ]
        query = Query("ACTUAL", """SELECT * FROM abc""", [], {"abc": "INPUT_DATA"})

        qlt = QueryLogicTest(client, expected, input_tables, query)
        success, records = qlt.run()
        assert not success
        assert records == [
            bigquery.Row(
                ("+", "ddd", "ccc", 400, 2),
                {"mark": 0, "name": 1, "category": 2, "value": 3, "n": 4},
            )
        ]

    @pytest.mark.skipif(is_githubactions(), reason="GitHub Actions")
    def test_期待しているテーブル結果の量に満たない場合マークがマイナスになる差分が出る(self):
        from google.cloud import bigquery

        client = bigquery.Client()

        expected = Table(
            str(Path(__file__).parent / "testdata/test5.json"),
            [
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                {"name": "value", "type": "INT64", "mode": "NULLABLE"},
            ],
            "EXPECTED",
        )
        input_tables = [
            Table(
                str(Path(__file__).parent / "testdata/test3.json"),
                [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "INPUT_DATA",
            )
        ]
        query = Query("ACTUAL", """SELECT * FROM abc""", [], {"abc": "INPUT_DATA"})
        qlt = QueryLogicTest(client, expected, input_tables, query)
        success, records = qlt.run()
        assert not success
        assert records == [
            bigquery.Row(
                ("-", "eee", "fff", 500, 3),
                {"mark": 0, "name": 1, "category": 2, "value": 3, "n": 4},
            )
        ]


class TestQueryTest:
    @pytest.mark.skipif(is_githubactions(), reason="GitHub Actions")
    def test_辞書データからクエリのテストが実行できる差分なし(self):
        from google.cloud import bigquery

        client = bigquery.Client()
        tables = {
            "hogehoge": {
                "schema": [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "datum": [["abc", 100]],
            },
            "fuga": {
                "schema": [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "datum": [["ddd", 100]],
            },
        }
        query = {
            "type": "query",
            "name": "ACTUAL",
            "query": "SELECT * FROM hogehoge UNION ALL SELECT * FROM fuga",
            "params": [],
        }
        expected = {
            "type": "data",
            "name": "EXPECTED",
            "schema": [
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "value", "type": "INT64", "mode": "NULLABLE"},
            ],
            "datum": [["abc", 100], ["ddd", 100]],
        }

        qt = QueryTest(client, expected, tables, query)
        success, diff = qt.run()
        assert success and diff == []

    @pytest.mark.skipif(is_githubactions(), reason="GitHub Actions")
    def test_辞書データからクエリのテストが実行できる差分あり(self):
        from google.cloud import bigquery

        client = bigquery.Client()
        tables = {
            "hogehoge": {
                "schema": [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "datum": [["abc", 100]],
            },
            "fuga": {
                "schema": [
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
                ],
                "datum": [["ddd", 100]],
            },
        }
        query = {
            "type": "query",
            "name": "ACTUAL",
            "query": "SELECT * FROM hogehoge UNION ALL SELECT * FROM fuga",
            "params": [],
        }
        expected = {
            "type": "data",
            "name": "EXPECTED",
            "schema": [
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "value", "type": "INT64", "mode": "NULLABLE"},
            ],
            "datum": [["abc", 100]],
        }

        qt = QueryTest(client, expected, tables, query)
        success, diff = qt.run()
        assert not success and diff != []

    @pytest.mark.skipif(is_githubactions(), reason="GitHub Actions")
    def test_クエリが誤っていてデータ走査量がゼロではないときにはAssertionError(self):
        from google.cloud import bigquery

        client = bigquery.Client()
        tables = {
            "fuga": {
                "schema": [{"name": "name", "type": "STRING", "mode": "NULLABLE"},],
                "datum": [["ddd"]],
            },
        }
        query = {
            "type": "query",
            "name": "ACTUAL",
            "query": "SELECT word_count FROM `bigquery-public-data.samples.shakespeare`",
            "params": [],
        }
        expected = {
            "type": "data",
            "name": "EXPECTED",
            "schema": [{"name": "word_count", "type": "INT64", "mode": "NULLABLE"},],
            "datum": [[1]],
        }

        qt = QueryTest(client, expected, tables, query)
        with pytest.raises(AssertionError):
            success, diff = qt.run()
