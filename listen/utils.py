import json
import itertools as it
from urllib import parse
import aiopg


def get_connection(uri: str) -> aiopg.utils._ContextManager[aiopg.pool.Pool]:
    result = parse.urlparse(uri)
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    port = result.port
    ctx = aiopg.create_pool(
        database = database,
        user = username,
        password = password,
        host = hostname,
        port = port
    )
    return ctx


def read_tables(file_name: str):
    with open(file_name) as file:
        sql_tables = json.load(file)
    
    assert isinstance(sql_tables, list), "deve ser uma lista"
    tables = it.chain.from_iterable(map(lambda i: [x for x in i.keys()], sql_tables))

    tables = filter(lambda x: x == "account_bank_info", tables)
    return tables