# Databricks Medallion Architecture Pipeline

An end-to-end data pipeline on Databricks Unity Catalog built on the **Medallion Architecture** — a layered approach that progressively refines raw data into trusted, analytics-ready assets across **Bronze → Silver → Gold** layers.

---

## Architecture

```mermaid
flowchart TD
    subgraph Sources["Data Sources (CSV)"]
        A1["airports.csv"]
        A2["bookings.csv"]
        A3["passengers.csv"]
    end

    subgraph Job["Databricks Job: ingest_csv_to_delta"]
        direction TB
        T1["Task 1\ningest_csv_to_delta"]
        T2["Task 2\nenrich_bookings"]
        T3["Task 3\ncreate_cur_views"]
        T1 --> T2 --> T3
    end

    subgraph BRONZE["BRONZE — claudecatalog.raw"]
        R1[("airports\n(Delta)")]
        R2[("bookings\n(Delta)")]
        R3[("passengers\n(Delta)")]
    end

    subgraph SILVER["SILVER — claudecatalog.enr"]
        E1[("bookings_enriched\n(Delta)")]
    end

    subgraph GOLD["GOLD — claudecatalog.cur"]
        C1[("bookings_per_city\n(View)")]
    end

    A1 --> T1
    A2 --> T1
    A3 --> T1

    T1 --> R1 & R2 & R3

    R2 -- "JOIN ON passenger_id" --> T2
    R3 -- "JOIN ON passenger_id" --> T2
    R2 -- "JOIN ON airport_id"   --> T2
    R1 -- "JOIN ON airport_id"   --> T2

    T2 --> E1
    E1 --> T3
    T3 --> C1

    style BRONZE fill:#cd7f32,color:#fff,stroke:#a0522d
    style SILVER fill:#c0c0c0,color:#222,stroke:#999
    style GOLD   fill:#ffd700,color:#222,stroke:#b8860b
```

---

## Layers

### Bronze — `claudecatalog.raw`
Raw data landed directly from CSV sources into Delta tables. No transformations — data is preserved as-is for reprocessing and auditability.

| Table | Columns |
|---|---|
| `airports` | `airport_id`, `airport_name`, `city`, `country` |
| `bookings` | `booking_id`, `passenger_id`, `flight_id`, `airport_id`, `amount`, `booking_date` |
| `passengers` | `passenger_id`, `name`, `gender`, `nationality` |

### Silver — `claudecatalog.enr`
Cleaned and enriched layer. The three Bronze tables are joined into one wide table, resolving foreign keys into descriptive attributes.

| Table | Description |
|---|---|
| `bookings_enriched` | One big table — bookings enriched with passenger and airport details |

**Join logic:**
```sql
bookings
  LEFT JOIN passengers ON bookings.passenger_id = passengers.passenger_id
  LEFT JOIN airports   ON bookings.airport_id   = airports.airport_id
```

### Gold — `claudecatalog.cur`
Aggregated, business-ready layer. Views are optimised for reporting and dashboards — no raw joins required by consumers.

| View | Description |
|---|---|
| `bookings_per_city` | Total number of bookings per city, ordered by highest volume |

```sql
SELECT city, COUNT(booking_id) AS booking_count
FROM claudecatalog.enr.bookings_enriched
GROUP BY city
ORDER BY booking_count DESC
```

---

## Pipeline Scripts

| File | Task | Layer |
|---|---|---|
| `ingest_csv_to_delta.py` | `ingest_csv_to_delta` | Bronze |
| `enrich_bookings.py` | `enrich_bookings` | Silver |
| `create_cur_views.py` | `create_cur_views` | Gold |

---

## Job: `ingest_csv_to_delta`

Three sequential tasks — each layer only runs if the previous one succeeds.

```
ingest_csv_to_delta  →  enrich_bookings  →  create_cur_views
     (Bronze)              (Silver)              (Gold)
```

- Runs on **serverless compute**
- Each task uses `spark_python_task`
- Dependency: `run_if: ALL_SUCCESS`
