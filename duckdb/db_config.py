duckdb_path = 'duckdb-orig/duckdb/build/release/duckdb'
db_file = 'ldbc_db_sf10.duckdb'

def format_value_duckdb(value, param_type):
    param_type = param_type.upper()
    if param_type in ["DATETIME", "TIMESTAMPTZ"]:
        value_sql = value.replace("T", " ")
        return f"TIMESTAMPTZ '{value_sql}'"
    elif param_type == "DATE":
        return f"DATE '{value}'"
    elif param_type in ["INT", "INT32", "INT64", "BIGINT", "ID"]:
        return value
    elif param_type == "STRING[]":
        items = value.split(";")
        quoted_items = [f"'{item}'" for item in items]
        return "[" + ", ".join(quoted_items) + "]"
    else:
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
 