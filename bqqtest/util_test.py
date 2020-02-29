import pytest

from .util import get_query_from_with_clause

params = {
    "WITHがないときは何も返さない": ("SELECT * FROM test", []),
    "WITHの中にクエリがひとつだけの場合はひとつ返す": (
        "WITH aaa AS (SELECT * FROM zzz) SELECT * FROM aaa",
        [("aaa", "SELECT * FROM zzz")],
    ),
    "WITHの中にクエリが複数ある場合はすべて返す": (
        "WITH aaa AS (SELECT * FROM zzz), bbb AS (SELECT ARRAY_LENGTH(yyy.ccc) FROM yyy) SELECT * FROM bbb",
        [
            ("aaa", "SELECT * FROM zzz"),
            ("bbb", "SELECT ARRAY_LENGTH(yyy.ccc) FROM yyy"),
        ],
    ),
    "マルチラインでもクエリを発見できる": (
        """#standardsql
-- テスト用のクエリ
WITH aaa AS (
    SELECT * FROM zzz
),
bbb AS (
    SELECT ARRAY_LENGTH(yyy.ccc) FROM yyy
)
SELECT * FROM bbb""",
        [
            ("aaa", "SELECT * FROM zzz"),
            ("bbb", "SELECT ARRAY_LENGTH(yyy.ccc) FROM yyy"),
        ],
    ),
    "コメントを無視してクエリを返す": (
        """#standardsql
-- テスト用のクエリ
WITH aaa AS (
    --- aaaというテーブル
    SELECT * FROM zzz
),
bbb AS (
    SELECT ARRAY_LENGTH(yyy.ccc) FROM yyy
)
-- 使っていないクエリ
# ほげほげ
/*,
ccc AS (
    SELECT * FROM xxxx # コメント
)*/

SELECT * FROM bbb""",
        [
            ("aaa", "SELECT * FROM zzz"),
            ("bbb", "SELECT ARRAY_LENGTH(yyy.ccc) FROM yyy"),
        ],
    ),
}


@pytest.mark.parametrize(
    ["sql", "want"], list(params.values()), ids=list(params.keys())
)
def test_get_query_from_with_clause(sql, want):
    got = get_query_from_with_clause(sql)
    assert want == got
