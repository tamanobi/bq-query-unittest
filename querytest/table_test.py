from .table import Table, ColumnMeta, Schema
from pathlib import Path
import pytest

class TestTable:
    def test_table(self):
        p = Path(__file__).parent / 'testdata/test1.csv'
        schema = [
            ('name', 'STRING'),
            ('category', 'STRING'),
            ('value', 'INT64'),
        ]

        assert Table(str(p), schema)

    def test_with句を生成できる(self):
        p = Path(__file__).parent / 'testdata/test1.csv'
        schema = [
            ('name', 'STRING'),
            ('category', 'STRING'),
            ('value', 'INT64'),
        ]
        t = Table(str(p), schema, 'TEST_DATA')
        w = t.to_with_clause()
        assert w == """WITH TEST_DATA AS (
SELECT * FROM UNNEST(ARRAY<STRUCT<name STRING, category STRING, value INT64>>
[("abc","bdc",200),("ほげほげ","ふがふが",300000)]
)
)"""

    def test_dataframe_to_string_list(self):
        p = Path(__file__).parent / 'testdata/test2.json'
        t = Table(str(p), [('name', 'STRING'),('category', 'STRING'),('value', 'INT64'),], 'TEST_DATA')

        assert t.dataframe_to_string_list() == [['"abc"', '"bcd"', '300'], ['"ddd"', '"ccc"', '400'], [r'"\"xxx\""', '"yyy"', '123'], [r'"\"xxx\""',r'"[\"y\",\"y\",\"y\"]"','123']]

    def test_sql_stringによってリストからsqlのレコードが生成できる(self):
        input_list = [['"abc"', '"bcd"', '300'], ['"ddd"', '"ccc"', '400'], [r'"\"xxx\""', '"yyy"', '123']]
        assert Table.sql_string(input_list) == r'[("abc","bcd",300),("ddd","ccc",400),("\"xxx\"","yyy",123)]'

class TestColumnMeta:
    def test_STRINGは使えるタイプ(self):
        assert ColumnMeta('name', 'STRING')

    def test_ARRAYは使えるタイプ(self):
        assert ColumnMeta('name', 'ARRAY<STRING>')

    def test_ARRAYにはカッコが必要(self):
        with pytest.raises(AssertionError):
            ColumnMeta('name', 'ARRAY')

    def test_strで文字列に変更できる(self):
        assert str(ColumnMeta('name', 'STRING')) == 'name STRING'

    def test_nameに改行や空白文字は入れられない(self):
        with pytest.raises(AssertionError):
            assert ColumnMeta('\n', 'STRING')
        with pytest.raises(AssertionError):
            assert ColumnMeta('a b', 'STRING')

    def test_使えないタイプを入れるとAssertionError(self):
        with pytest.raises(AssertionError):
            ColumnMeta('name', 'INTEGER')

    def test_空文字列はAssertionError(self):
        with pytest.raises(AssertionError):
            ColumnMeta('', 'ARRAY<STRING>')

    @pytest.mark.skip(reason='ARRAYの直下にARRAYは入れられないが、バグっている')
    def test_ARRAYの直下にARRAYは入れられない(self):
        with pytest.raises(AssertionError):
            ColumnMeta('name', 'ARRAY<ARRAY<STRING>>')

class TestSchema:
    def test_スキーマを生成できる(self):
        assert Schema([('name', 'STRING'), ('value', 'INT64')])

    def test_スキーマは空配列から生成しようとするとAssertionErrorになる(self):
        with pytest.raises(AssertionError):
            assert Schema([])

    def test_スキーマはstrでSTRUCTに変換できる(self):
        s = Schema([('time', 'TIMESTAMP'), ('event', 'STRING'), ('id', 'INT64')])
        assert str(s) == 'STRUCT<time TIMESTAMP, event STRING, id INT64>'
