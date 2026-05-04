from pyspark.sql import SparkSession

CATALOG = "claudecatalog"
ENR = "enr"
CUR = "cur"


def create_cur_views():
    spark = SparkSession.builder.getOrCreate()

    spark.sql(f"""
        CREATE OR REPLACE VIEW {CATALOG}.{CUR}.bookings_per_city AS
        SELECT
            city,
            COUNT(booking_id) AS booking_count
        FROM {CATALOG}.{ENR}.bookings_enriched
        GROUP BY city
        ORDER BY booking_count DESC
    """)
    print(f"View {CATALOG}.{CUR}.bookings_per_city created successfully.")


if __name__ == "__main__":
    create_cur_views()
