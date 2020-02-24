# WIP: BigQueryのクエリをテストするためのツール
<img alt="Run pytest" src="https://github.com/tamanobi/bq-query-unittest/workflows/Run%20Tests/badge.svg">

BigQueryへのクエリロジックのテストができます

## 特徴

 * CSV形式のファイルを元にBigQueryにテーブルを一時的に作成します
 * 一時的に作成したテーブルに対して、クエリを発行し、結果を得ます
 * 結果と、期待しているテーブル(CSVファイル)と突合し、違いがなければテストをパスします

## 注意
BigQueryへ直接クエリを発行します。
