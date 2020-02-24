# WIP: BigQueryのクエリをテストするためのツール
<img alt="Run pytest" src="https://github.com/tamanobi/bq-query-unittest/workflows/Run%20Tests/badge.svg">

BigQueryへのクエリロジックのテストができます

# Usage

```python
from bqqtest import QueryTest
from google.cloud import bigquery

expected = {'schema': [('name', 'STRING'), ('value', 'INT64')], 'datum': [['abc', 100]]}
tables = [{'schema': [('name', 'STRING'), ('value', 'INT64')], 'datum': [['abc', 100]], 'name': 'INPUT_DATA'}]
query = {'query': 'SELECT * FROM hogehoge', 'map': {'hogehoge': 'INPUT_DATA'}, 'params': []}
qt = QueryTest(bigquery.Client(), expected, tables, query)
success, diff = qt.run()
success # True
```

## 特徴

 * WITHを利用してテストデータを一時的に生成します。このデータはBigQueryに保存されません。BigQueryは保存されているデータ走査した量とAPIリクエスト数で課金されるため、課金額を抑えた状態でテストできます

## 注意
BigQueryへ直接クエリを発行します。
