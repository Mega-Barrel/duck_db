"""DuckDB Code"""

import time
import duckdb
import pandas as pd

if __name__ == "__main__":
    cursor = duckdb.connect()

    cur_time = time.time()
    df = cursor.execute(
        """
            SELECT *
            FROM read_csv('data/Sales_Product_Combined.csv')
            LIMIT 10
        """
    ).df()
    print(f"time: {round((time.time() - cur_time), 2)}s")
    print(df)
    print(df.dtypes)

    # Product wise Quantity Ordered and Price
    cur_time = time.time()
    agg_view = cursor.execute(
        """
            SELECT
                Product,
                SUM(Quantity Ordered) AS quantity
            FROM
                df
            GROUP BY
                1
            ORDER BY
                2 DESC
        """
    ).df()
    print(f"time: {round((time.time() - cur_time), 2)}s")
    print(df)
