
import datetime


def wrap_value(value) -> str:
    if value is None:
        return "NULL"
    elif isinstance(value, datetime.datetime):
        return datetime.datetime.strftime(value, "'%Y-%m-%d %H:%M:%S'")
    elif isinstance(value, datetime.date):
        return datetime.datetime.strftime(value, "'%Y-%m-%d'")
    elif isinstance(value, str):
        return "'%s'" % value.replace("'", "''")
    else:
        return str(value)
