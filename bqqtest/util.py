import regex


f = regex.compile(
    r"(?<name>\w+)\s+AS\s+(?<query>\((?:[^\(\)]+|(?&query))*\))",
    flags=regex.MULTILINE | regex.IGNORECASE,
)


def get_query_from_with_clause(sql: str):
    assert isinstance(sql, str)
    sql = regex.sub(r"--.*$", "", sql, flags=regex.MULTILINE | regex.IGNORECASE)
    sql = regex.sub(r"#.*$", "", sql, flags=regex.MULTILINE | regex.IGNORECASE)
    sql = regex.sub(
        r"/\*.*\*/", "", sql, flags=regex.MULTILINE | regex.IGNORECASE | regex.DOTALL
    )
    print(sql)

    queries = regex.findall(f, sql)

    # ()が先頭と末尾に入ってくるので取り除く
    return [(name, query[1:][:-1].strip()) for name, query in queries]
