"""DuckDB Code"""

import duckdb

if __name__ == "__main__":
    cursor = duckdb.connect()
    res = cursor.execute('SELECT 42, 43').fetchall()

    print(res)
