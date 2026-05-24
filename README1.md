# Week 1 — India AQI ETL Pipeline

**Stack:** Python · pandas · SQLite  
**Time to run:** < 5 seconds  
**Data:** 3,650 rows · 10 Indian cities · 365 days of AQI readings

## Run it

```bash
pip install pandas
python generate_data.py    # creates city_day.csv
python etl_pipeline.py     # runs ETL, creates air_quality.db
```

## What it does

```
city_day.csv
    │
    ▼  extract()   — reads CSV, logs shape + null count
    │
    ▼  transform() — fills nulls with city median
    │               — adds AQI_Category, Month, Season
    │               — aggregates to monthly summary
    │
    ▼  load()      — writes raw_aqi table to SQLite
    │               — writes monthly_aqi table
    │               — creates city_annual SQL view
    │
    ▼  verify()    — queries DB with pandas, prints results
```

## Tables created in air_quality.db

| Table/View | Rows | Description |
|---|---|---|
| `raw_aqi` | 3,650 | Cleaned daily readings, all cities |
| `monthly_aqi` | 120 | Aggregated monthly stats per city |
| `city_annual` | 10 | SQL view — annual ranking |

## Key concepts learned (by doing)

- `pd.read_csv()` with `parse_dates` → no manual conversion
- `groupby().transform('median')` → city-specific null fill
- `apply()` with a function → AQI categorisation
- `to_sql()` / `read_sql()` → pandas ↔ database bridge
- SQL `CREATE VIEW` → reusable saved queries
- Logging pattern → every production pipeline uses this

## Resume bullet for this project

> Built an end-to-end ETL pipeline in Python (pandas + SQLite) processing 3,650 records across 10 Indian cities — extract from CSV, transform with null handling and feature engineering, load to relational DB with SQL views for analysis.

## Mainframe connection

| Mainframe | Python pipeline |
|---|---|
| QSAM file read | `pd.read_csv()` |
| COBOL PERFORM loop | `df.apply(fn)` |
| SORT FIELDS | `df.sort_values()` |
| COND check | `try/except` + logging |
| DB2 INSERT | `df.to_sql()` |
| JCL steps | `extract() → transform() → load()` |

## Next — Week 2

Replace SQLite with PostgreSQL. Call a live REST API instead of a CSV. Add psycopg2 connector.
