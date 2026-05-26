"""
Week 1 — India Air Quality ETL Pipeline
Day 2 update: seasonal analysis, window functions, JSON export

Stack: pandas + SQLite (zero cloud, zero cost)
Run:   python etl_pipeline.py

Day 1 taught: extract → transform → load → verify
Day 2 adds:   4 new SQL queries including window functions + JSON output
"""
import pandas as pd
import sqlite3
import logging
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    datefmt='%H:%M:%S'
)


# ── EXTRACT ───────────────────────────────────────────────────────────────────
def extract(filepath: str) -> pd.DataFrame:
    """Read raw CSV. Always check shape + nulls first."""
    logging.info(f"EXTRACT  reading {filepath}")
    df = pd.read_csv(filepath, parse_dates=['Date'])
    logging.info(f"EXTRACT  shape={df.shape}  nulls={df.isnull().sum().sum()}")
    return df


# ── TRANSFORM ─────────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame):
    """Clean, enrich, and aggregate the raw data."""
    logging.info("TRANSFORM  starting")

    # 1. Fill nulls with city-specific median (smarter than global median)
    df['AQI'] = df['AQI'].fillna(
        df.groupby('City')['AQI'].transform('median')
    )
    logging.info(f"TRANSFORM  nulls fixed — remaining: {df['AQI'].isna().sum()}")

    # 2. Categorise AQI — Central Pollution Control Board (CPCB) scale
    def aqi_category(aqi):
        if   aqi <= 50:  return 'Good'
        elif aqi <= 100: return 'Satisfactory'
        elif aqi <= 200: return 'Moderate'
        elif aqi <= 300: return 'Poor'
        else:            return 'Severe'

    df['AQI_Category'] = df['AQI'].apply(aqi_category)

    # 3. Time dimensions — used by every downstream SQL query
    df['Month']  = df['Date'].dt.month
    df['Year']   = df['Date'].dt.year
    df['Season'] = df['Month'].map({
        12: 'Winter',  1: 'Winter',  2: 'Winter',
         3: 'Spring',  4: 'Spring',  5: 'Spring',
         6: 'Monsoon', 7: 'Monsoon', 8: 'Monsoon',
         9: 'Post-Monsoon', 10: 'Post-Monsoon', 11: 'Post-Monsoon',
    })

    # 4. Monthly aggregation — what analysts and dashboards consume
    monthly = (
        df.groupby(['City', 'Year', 'Month', 'Season'])
          .agg(
              avg_aqi   = ('AQI',   'mean'),
              max_aqi   = ('AQI',   'max'),
              min_aqi   = ('AQI',   'min'),
              days_poor = ('AQI',   lambda x: (x > 200).sum()),
              avg_pm25  = ('PM2.5', 'mean'),
          )
          .round(2)
          .reset_index()
    )

    logging.info(f"TRANSFORM  monthly summary: {len(monthly)} rows")
    return df, monthly


# ── LOAD ──────────────────────────────────────────────────────────────────────
def load(raw_df: pd.DataFrame, monthly_df: pd.DataFrame, db_path: str):
    """Write both tables to SQLite and create SQL views."""
    logging.info(f"LOAD  writing to {db_path}")
    conn = sqlite3.connect(db_path)

    raw_df.to_sql('raw_aqi',      conn, if_exists='replace', index=False)
    monthly_df.to_sql('monthly_aqi', conn, if_exists='replace', index=False)

    conn.execute("DROP VIEW IF EXISTS city_annual")
    conn.execute("""
        CREATE VIEW city_annual AS
        SELECT
            City,
            Year,
            ROUND(AVG(avg_aqi), 1) AS annual_avg_aqi,
            SUM(days_poor)         AS total_poor_days
        FROM  monthly_aqi
        GROUP BY City, Year
    """)

    conn.commit()
    conn.close()
    logging.info(
        f"LOAD  done — raw_aqi({len(raw_df)}), "
        f"monthly_aqi({len(monthly_df)}), view: city_annual"
    )


# ── VERIFY — DAY 1 queries ────────────────────────────────────────────────────
def verify_day1(conn: sqlite3.Connection):
    print("\n" + "="*55)
    print("  TOP 10 CITIES BY ANNUAL AQI")
    print("="*55)
    print(pd.read_sql("""
        SELECT City, annual_avg_aqi, total_poor_days
        FROM   city_annual
        ORDER  BY annual_avg_aqi DESC
    """, conn).to_string(index=False))

    print("\n" + "="*55)
    print("  BENGALURU — MONTHLY TREND")
    print("="*55)
    print(pd.read_sql("""
        SELECT Month, Season, avg_aqi, days_poor
        FROM   monthly_aqi
        WHERE  City = 'Bengaluru'
        ORDER  BY Month
    """, conn).to_string(index=False))

    print("\n" + "="*55)
    print("  AQI CATEGORY DISTRIBUTION (ALL CITIES)")
    print("="*55)
    print(pd.read_sql("""
        SELECT AQI_Category, COUNT(*) AS days
        FROM   raw_aqi
        GROUP  BY AQI_Category
        ORDER  BY days DESC
    """, conn).to_string(index=False))


# ── VERIFY — DAY 2 queries (NEW) ──────────────────────────────────────────────
def verify_day2(conn: sqlite3.Connection):

    # Query 1: Seasonal analysis — which season is worst?
    print("\n" + "="*55)
    print("  WORST SEASON BY AQI (ALL CITIES)")
    print("="*55)
    print(pd.read_sql("""
        SELECT
            Season,
            ROUND(AVG(avg_aqi), 1) AS season_avg_aqi,
            SUM(days_poor)         AS total_poor_days
        FROM   monthly_aqi
        GROUP  BY Season
        ORDER  BY season_avg_aqi DESC
    """, conn).to_string(index=False))
    # What you learn: GROUP BY on a derived column, multi-metric aggregation

    # Query 2: Window function — rank cities + compare vs national average
    # WINDOW FUNCTIONS are asked in every DE interview. Learn this pattern.
    print("\n" + "="*55)
    print("  CITY POLLUTION RANK + vs NATIONAL AVG")
    print("  (Window function: RANK + AVG OVER)")
    print("="*55)
    print(pd.read_sql("""
        SELECT
            City,
            annual_avg_aqi,
            RANK() OVER (ORDER BY annual_avg_aqi DESC)      AS pollution_rank,
            ROUND(annual_avg_aqi
                  - AVG(annual_avg_aqi) OVER (), 1)         AS vs_national_avg
        FROM city_annual
    """, conn).to_string(index=False))
    # What you learn: RANK() OVER, AVG() OVER () — window without PARTITION

    # Query 3: ROW_NUMBER with PARTITION — worst month PER city
    print("\n" + "="*55)
    print("  WORST MONTH PER CITY")
    print("  (Window function: ROW_NUMBER PARTITION BY)")
    print("="*55)
    print(pd.read_sql("""
        SELECT City, Month, Season, avg_aqi
        FROM (
            SELECT
                City, Month, Season, avg_aqi,
                ROW_NUMBER() OVER (
                    PARTITION BY City
                    ORDER BY avg_aqi DESC
                ) AS rn
            FROM monthly_aqi
        )
        WHERE rn = 1
        ORDER BY avg_aqi DESC
    """, conn).to_string(index=False))
    # What you learn: ROW_NUMBER PARTITION BY = "rank within each group"
    # Interview tip: "Get the top N per group" — always use this pattern

    # Query 4: Export summary as JSON — API-ready output
    # Real pipelines often write JSON for downstream APIs or dashboards
    print("\n" + "="*55)
    print("  JSON EXPORT — API-READY OUTPUT")
    print("="*55)
    result = pd.read_sql("""
        SELECT City, annual_avg_aqi, total_poor_days
        FROM   city_annual
        ORDER  BY annual_avg_aqi DESC
    """, conn)
    summary = result.to_dict(orient='records')
    with open('aqi_summary.json', 'w') as f:
        json.dump({'generated_at': datetime.now().isoformat(),
                   'cities': summary}, f, indent=2)
    print(json.dumps(summary[:3], indent=2))
    print(f"... {len(summary)} cities — saved to aqi_summary.json")
    # What you learn: to_dict(orient='records') = list of dicts = JSON-ready


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    t0 = datetime.now()
    logging.info("India AQI ETL Pipeline — Day 2")

    raw_df             = extract('city_day.csv')
    raw_df, monthly_df = transform(raw_df)
    load(raw_df, monthly_df, 'air_quality.db')

    conn = sqlite3.connect('air_quality.db')
    verify_day1(conn)   # Day 1 queries — unchanged
    verify_day2(conn)   # Day 2 queries — NEW
    conn.close()

    elapsed = (datetime.now() - t0).total_seconds()
    logging.info(f"Pipeline complete in {elapsed:.2f}s")
    logging.info("New file: aqi_summary.json — API-ready output")