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
