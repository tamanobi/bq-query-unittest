# BigQueryのクエリをテストするためのツール
<img alt="Run pytest" src="https://github.com/tamanobi/bq-query-unittest/workflows/Run%20Tests/badge.svg">

BigQueryへのクエリロジックのテストができます

## Basic Usage

### Simple

```python
from bqqtest import QueryTest
from google.cloud import bigquery

# expected
expected_schema = [
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
]
expected_datum = [["abc", 100], ["bbb", 333]]
expected = {"schema": expected_schema, "datum": expected_datum}

# actual
target_schema = [
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
]
target_datum = [["abc", 100], ["bbb", 333]]
tables = {"test.target_table": {"schema": target_schema, "datum": target_datum}}
eval_query = {"query": "SELECT * FROM test.target_table", "params": []}

qt = QueryTest(bigquery.Client(), expected, tables, eval_query)
success, diff = qt.run()
success  # True
```

## Group By

```python
from bqqtest import QueryTest
from google.cloud import bigquery

# expected
expected_schema = [
    {"name": "item", "type": "STRING", "mode": "NULLABLE"},
    {"name": "total", "type": "INT64", "mode": "NULLABLE"},
]
expected_datum = [["abc", 300], ["bbb", 333]]
expected = {"schema": expected_schema, "datum": expected_datum}

# actual
target_schema = [
    {"name": "item", "type": "STRING", "mode": "NULLABLE"},
    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
]
target_datum = [["abc", 100], ["bbb", 333], ["abc", 200]]
tables = {"test.target_table": {"schema": target_schema, "datum": target_datum}}
eval_query = {
    "query": "SELECT item, SUM(value) AS total FROM test.target_table GROUP BY item",
    "params": [],
}

qt = QueryTest(bigquery.Client(), expected, tables, eval_query)
success, diff = qt.run()
success  # True
```

## Multi Table

```python
from bqqtest import QueryTest
from google.cloud import bigquery


# expected
expected_schema = [
    {"name": "item", "type": "STRING", "mode": "NULLABLE"},
    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
]
expected_datum = [["abc", 100], ["bbb", 333], ["xxxx", 888], ["zzzz", 999]]
expected = {"schema": expected_schema, "datum": expected_datum}

# actual
target_schema = [
    {"name": "item", "type": "STRING", "mode": "NULLABLE"},
    {"name": "value", "type": "INT64", "mode": "NULLABLE"},
]
target_datum1 = [["abc", 100], ["bbb", 333]]
target_datum2 = [["xxxx", 888], ["zzzz", 999]]
tables = {
    "test.table1": {"schema": target_schema, "datum": target_datum1},
    "test.table2": {"schema": target_schema, "datum": target_datum2},
}
eval_query = {
    "query": "SELECT * FROM `test.table1` UNION ALL SELECT * FROM `test.table2`",
    "params": [],
}

qt = QueryTest(bigquery.Client(), expected, tables, eval_query)
success, diff = qt.run()
success  # True
```

## 特徴

see also https://qiita.com/tamanobi/items/9434ca0dbd5f0d3018d9

 * WITH を利用して、 BigQuery に保存されないテストデータを一時的に生成します。
    * BigQuery は保存されているデータ走査した量とAPIリクエスト数で課金されるため、費用抑えてユニットテストができます。
    * 料金の詳細は、 BigQuery の公式ドキュメントを参照してください
 * テストをするために、クエリを書き直す必要はありません
    * ライブラリ内部では、対象テーブルの Identifier を書き換えてテーブルを差し替えます

## 注意

BigQuery へ直接クエリを発行します。
