import pandas as pd
from pyspark.sql import SparkSession


def ingest_csv_to_delta(urls: dict, catalog: str, schema: str, mode: str = "overwrite"):
    """
    Fetch CSV files from URLs and save as Delta tables in Databricks Unity Catalog.

    Args:
        urls: dict of {table_name: url}
        catalog: Unity Catalog name (e.g. "claudecatalog")
        schema: Schema name (e.g. "raw")
        mode: Write mode — "overwrite" or "append"
    """
    spark = SparkSession.builder.getOrCreate()

    for table_name, url in urls.items():
        print(f"Fetching {table_name} from: {url}")
        pdf = pd.read_csv(url)
        print(f"  Rows: {len(pdf)}, Columns: {list(pdf.columns)}")

        sdf = spark.createDataFrame(pdf)

        full_table = f"{catalog}.{schema}.{table_name}"
        sdf.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(full_table)
        print(f"  Saved to Delta table: {full_table}\n")

    print("Done.")


if __name__ == "__main__":
    urls = {
        "airports":   "https://raw.githubusercontent.com/anshlambagit/Claude_X_Dtabricks/refs/heads/main/airports.csv",
        "bookings":   "https://raw.githubusercontent.com/anshlambagit/Claude_X_Dtabricks/refs/heads/main/bookings.csv",
        "passengers": "https://raw.githubusercontent.com/anshlambagit/Claude_X_Dtabricks/refs/heads/main/passengers.csv",
    }

    ingest_csv_to_delta(urls=urls, catalog="claudecatalog", schema="raw")
