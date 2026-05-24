"""
Week 1 — India Air Quality ETL Pipeline
Stack: pandas + SQLite (zero cloud, zero cost)
Run:   python etl_pipeline.py

What this teaches you (by doing, not watching):
  extract()   = read raw CSV, report shape & nulls
  transform() = clean nulls, add derived columns, aggregate
  load()      = write to SQLite tables + create a SQL view
  verify()    = query the DB with pandas, print results
"""
import pandas as pd
import sqlite3
import logging
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

    # 1. Fill nulls — use city median, not global median (smarter)
    df['AQI'] = df['AQI'].fillna(
        df.groupby('City')['AQI'].transform('median')
    )
    logging.info(f"TRANSFORM  nulls fixed — remaining: {df['AQI'].isna().sum()}")

    # 2. Categorise AQI (Central Pollution Control Board scale)
    def aqi_category(aqi):
        if   aqi <= 50:  return 'Good'
        elif aqi <= 100: return 'Satisfactory'
        elif aqi <= 200: return 'Moderate'
        elif aqi <= 300: return 'Poor'
        else:            return 'Severe'

    df['AQI_Category'] = df['AQI'].apply(aqi_category)

    # 3. Add time dimensions — useful for all future SQL queries
    df['Month']  = df['Date'].dt.month
    df['Year']   = df['Date'].dt.year
    df['Season'] = df['Month'].map({
        12: 'Winter',  1: 'Winter',  2: 'Winter',
         3: 'Spring',  4: 'Spring',  5: 'Spring',
         6: 'Monsoon', 7: 'Monsoon', 8: 'Monsoon',
         9: 'Post-Monsoon', 10: 'Post-Monsoon', 11: 'Post-Monsoon',
    })

    # 4. Monthly summary table — this is what analysts/dashboards consume
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
    """Write both tables into SQLite and create an analyst view."""
    logging.info(f"LOAD  writing to {db_path}")
    conn = sqlite3.connect(db_path)

    raw_df.to_sql('raw_aqi',     conn, if_exists='replace', index=False)
    monthly_df.to_sql('monthly_aqi', conn, if_exists='replace', index=False)

    # SQL view = a saved query. Analysts query this, not the raw table.
    conn.execute("DROP VIEW IF EXISTS city_annual")
    conn.execute("""
        CREATE VIEW city_annual AS
        SELECT
            City,
            Year,
            ROUND(AVG(avg_aqi), 1)  AS annual_avg_aqi,
            SUM(days_poor)          AS total_poor_days
        FROM  monthly_aqi
        GROUP BY City, Year
    """)

    conn.commit()
    conn.close()
    logging.info(
        f"LOAD  done — raw_aqi({len(raw_df)} rows), "
        f"monthly_aqi({len(monthly_df)} rows), view: city_annual"
    )


# ── VERIFY ────────────────────────────────────────────────────────────────────
def verify(db_path: str):
    """Query the DB with pandas — proves the pipeline worked."""
    conn = sqlite3.connect(db_path)

    print("\n" + "="*55)
    print("  TOP 10 CITIES BY AQI 2023")
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

    conn.close()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    t0 = datetime.now()
    logging.info("India AQI ETL Pipeline — starting")

    raw_df               = extract('city_day.csv')
    raw_df, monthly_df   = transform(raw_df)
    load(raw_df, monthly_df, 'air_quality.db')
    verify('air_quality.db')

    elapsed = (datetime.now() - t0).total_seconds()
    logging.info(f"Pipeline complete in {elapsed:.2f}s")
    logging.info("Files created: city_day.csv, air_quality.db")
    logging.info("Next: open air_quality.db in DB Browser for SQLite to explore")
