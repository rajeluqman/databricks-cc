from pyspark.sql import SparkSession

CATALOG = "claudecatalog"
RAW = "raw"
ENR = "enr"


def enrich_bookings():
    spark = SparkSession.builder.getOrCreate()

    bookings = spark.table(f"{CATALOG}.{RAW}.bookings")
    passengers = spark.table(f"{CATALOG}.{RAW}.passengers")
    airports = spark.table(f"{CATALOG}.{RAW}.airports")

    enriched = (
        bookings
        .join(passengers, on="passenger_id", how="left")
        .join(airports, on="airport_id", how="left")
    )

    target = f"{CATALOG}.{ENR}.bookings_enriched"
    enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"Written {enriched.count()} rows to {target}")


if __name__ == "__main__":
    enrich_bookings()
