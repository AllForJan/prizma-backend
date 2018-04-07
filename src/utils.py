import settings


def load_sql(query_name, kwargs):
    path = settings.SQL_FOLDER + query_name
    with open(path, 'r') as f:
        sql = f.read()
    if kwargs:
        sql = sql.format(**kwargs)
    return sql
