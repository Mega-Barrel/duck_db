"""DuckDB Code"""

import time
import duckdb

if __name__ == "__main__":
    cursor = duckdb.connect()

    cur_time = time.time()
    df = cursor.execute(
        """
            SELECT *
            FROM read_csv(
                'data/Sales_Product_Combined.csv',
                normalize_names=True,
                header = true,
                delim = ',',
                columns = {
                    'Order_ID': 'BIGINT',
                    'Product': 'VARCHAR',
                    'Quantity_Ordered': 'BIGINT',
                    'Price': 'DOUBLE',
                    'Order_Date': 'DATE',
                    'Time': 'VARCHAR',
                    'Purchase_Address': 'VARCHAR',
                    'Cxity': 'VARCHAR',
                    'Product_Type': 'VARCHAR'
                }
            )
        """
    ).df()
    print(f"time took to read csv file: {round((time.time() - cur_time), 2)}s")
    print(df)
    print()

    # Product wise Quantity Ordered and Price
    cur_time = time.time()
    agg_view = cursor.execute(
        """
            SELECT
                Product_Type AS product_type,
                ROUND(SUM(Price), 2) AS total_sales
            FROM
                df
            GROUP BY
                1
            ORDER BY
                2 DESC
            LIMIT
                5
        """
    ).fetchdf()
    print(f"time: {round((time.time() - cur_time), 2)}s")
    print(agg_view)

    # Daily Sales
    cur_time = time.time()
    daily_product_sales = cursor.execute(
        """
            SELECT
                Order_Date,
                Product,
                ROUND(SUM(Price), 2) AS daily_sales
            FROM
                df
            GROUP BY
                1, 2
            ORDER BY
                1 ASC,
                2 ASC
        """
    ).fetchdf()
    print(f"time: {round((time.time() - cur_time), 2)}s")
    print(daily_product_sales)
