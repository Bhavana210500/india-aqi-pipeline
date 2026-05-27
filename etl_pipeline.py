"""
Week 1 — India Air Quality ETL Pipeline
Day 3: LAG/LEAD, Rolling averages, CTEs, Data Quality report, Pipeline report

Stack : pandas + SQLite (zero cloud, zero cost)
Run   : python etl_pipeline.py

Day 1: extract → transform → load → basic queries
Day 2: seasonal analysis, window functions (RANK, ROW_NUMBER), JSON export
Day 3: LAG/LEAD trend, rolling avg, CTEs, data quality checks, pipeline report
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
    """Read raw CSV. Capture null count BEFORE cleaning — needed for DQ report."""
    logging.info(f"EXTRACT  reading {filepath}")
    df = pd.read_csv(filepath, parse_dates=['Date'])
    nulls = int(df['AQI'].isna().sum())
    dupes = int(df.duplicated(subset=['City', 'Date']).sum())
    logging.info(f"EXTRACT  shape={df.shape}  nulls={nulls}  dupes={dupes}")
    return df, nulls, dupes          # return nulls & dupes for DQ report


# ── TRANSFORM ─────────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame):
    """Clean, enrich, and aggregate."""
    logging.info("TRANSFORM  starting")

    # 1. Null fix — city-specific median
    df['AQI'] = df['AQI'].fillna(
        df.groupby('City')['AQI'].transform('median')
    )

    # 2. AQI Category (CPCB scale)
    def aqi_category(aqi):
        if   aqi <= 50:  return 'Good'
        elif aqi <= 100: return 'Satisfactory'
        elif aqi <= 200: return 'Moderate'
        elif aqi <= 300: return 'Poor'
        else:            return 'Severe'

    df['AQI_Category'] = df['AQI'].apply(aqi_category)

    # 3. Time dimensions
    df['Month']  = df['Date'].dt.month
    df['Year']   = df['Date'].dt.year
    df['Season'] = df['Month'].map({
        12: 'Winter',  1: 'Winter',  2: 'Winter',
         3: 'Spring',  4: 'Spring',  5: 'Spring',
         6: 'Monsoon', 7: 'Monsoon', 8: 'Monsoon',
         9: 'Post-Monsoon', 10: 'Post-Monsoon', 11: 'Post-Monsoon',
    })

    # 4. Monthly aggregation
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

    # 5. NEW DAY 3 — rolling 3-month avg per city using pandas
    #    Pandas rolling is easier than SQL for sliding windows
    monthly = monthly.sort_values(['City', 'Month'])
    monthly['rolling_3m_aqi'] = (
        monthly.groupby('City')['avg_aqi']
               .transform(lambda x: x.rolling(window=3, min_periods=1).mean())
               .round(2)
    )

    logging.info(f"TRANSFORM  monthly rows={len(monthly)}  rolling_3m added")
    return df, monthly


# ── LOAD ──────────────────────────────────────────────────────────────────────
def load(raw_df: pd.DataFrame, monthly_df: pd.DataFrame, db_path: str):
    """Write tables and views to SQLite."""
    logging.info(f"LOAD  writing to {db_path}")
    conn = sqlite3.connect(db_path)

    raw_df.to_sql('raw_aqi',         conn, if_exists='replace', index=False)
    monthly_df.to_sql('monthly_aqi', conn, if_exists='replace', index=False)

    conn.execute("DROP VIEW IF EXISTS city_annual")
    conn.execute("""
        CREATE VIEW city_annual AS
        SELECT City, Year,
               ROUND(AVG(avg_aqi), 1) AS annual_avg_aqi,
               SUM(days_poor)         AS total_poor_days
        FROM   monthly_aqi
        GROUP  BY City, Year
    """)

    conn.commit()
    conn.close()
    logging.info(f"LOAD  raw_aqi({len(raw_df)})  monthly_aqi({len(monthly_df)})")


# ── VERIFY DAY 1 ──────────────────────────────────────────────────────────────
def verify_day1(conn):
    print("\n" + "="*58)
    print("  TOP 10 CITIES BY ANNUAL AQI")
    print("="*58)
    print(pd.read_sql("""
        SELECT City, annual_avg_aqi, total_poor_days
        FROM   city_annual ORDER BY annual_avg_aqi DESC
    """, conn).to_string(index=False))

    print("\n" + "="*58)
    print("  AQI CATEGORY DISTRIBUTION")
    print("="*58)
    print(pd.read_sql("""
        SELECT AQI_Category, COUNT(*) AS days
        FROM   raw_aqi
        GROUP  BY AQI_Category ORDER BY days DESC
    """, conn).to_string(index=False))


# ── VERIFY DAY 2 ──────────────────────────────────────────────────────────────
def verify_day2(conn):
    print("\n" + "="*58)
    print("  WORST SEASON BY AQI")
    print("="*58)
    print(pd.read_sql("""
        SELECT Season,
               ROUND(AVG(avg_aqi), 1) AS season_avg_aqi,
               SUM(days_poor)         AS total_poor_days
        FROM   monthly_aqi
        GROUP  BY Season ORDER BY season_avg_aqi DESC
    """, conn).to_string(index=False))

    print("\n" + "="*58)
    print("  WORST MONTH PER CITY  (ROW_NUMBER PARTITION BY)")
    print("="*58)
    print(pd.read_sql("""
        SELECT City, Month, Season, avg_aqi FROM (
            SELECT City, Month, Season, avg_aqi,
                   ROW_NUMBER() OVER (
                       PARTITION BY City ORDER BY avg_aqi DESC
                   ) AS rn
            FROM monthly_aqi
        ) WHERE rn = 1 ORDER BY avg_aqi DESC
    """, conn).to_string(index=False))


# ── VERIFY DAY 3 — NEW ────────────────────────────────────────────────────────
def verify_day3(conn, raw_df, nulls_before, dupes_before):

    # ── Query 1: LAG — month-over-month change ────────────────────────────────
    # LAG() looks at the PREVIOUS row's value
    # Use case: detect sudden AQI spikes month-on-month
    print("\n" + "="*58)
    print("  LAG — MONTH-OVER-MONTH AQI CHANGE (Delhi)")
    print("  Pattern: LAG(col) OVER (PARTITION BY City ORDER BY Month)")
    print("="*58)
    print(pd.read_sql("""
        SELECT
            City, Month, Season,
            avg_aqi,
            LAG(avg_aqi) OVER (
                PARTITION BY City ORDER BY Month
            )                                          AS prev_month_aqi,
            ROUND(avg_aqi - LAG(avg_aqi) OVER (
                PARTITION BY City ORDER BY Month
            ), 1)                                      AS mom_change
        FROM  monthly_aqi
        WHERE City = 'Delhi'
        ORDER BY Month
    """, conn).to_string(index=False))
    # Interview tip: LEAD() does the same but looks FORWARD (next row)
    # "Find months where AQI jumped more than 20 points" = ABS(mom_change) > 20

    # ── Query 2: Rolling 3-month avg (from pandas transform in Day 3) ─────────
    print("\n" + "="*58)
    print("  ROLLING 3-MONTH AVERAGE — BENGALURU")
    print("  (computed in pandas during transform step)")
    print("="*58)
    print(pd.read_sql("""
        SELECT Month, Season, avg_aqi, rolling_3m_aqi
        FROM   monthly_aqi
        WHERE  City = 'Bengaluru'
        ORDER  BY Month
    """, conn).to_string(index=False))
    # Rolling avg smooths out spikes — used in time-series monitoring

    # ── Query 3: CTE — multi-step SQL in one query ────────────────────────────
    # CTE = WITH clause. Break complex queries into named steps.
    # Mainframe parallel: COBOL working storage variables for intermediate calc
    print("\n" + "="*58)
    print("  CTE — CITIES VS NATIONAL AVERAGE")
    print("  Pattern: WITH step1 AS (...), step2 AS (...) SELECT ...")
    print("="*58)
    print(pd.read_sql("""
        WITH national AS (
            -- Step 1: compute national average (one number)
            SELECT ROUND(AVG(annual_avg_aqi), 1) AS nat_avg
            FROM   city_annual
        ),
        ranked AS (
            -- Step 2: rank all cities
            SELECT City, annual_avg_aqi, total_poor_days,
                   RANK() OVER (ORDER BY annual_avg_aqi DESC) AS rank
            FROM   city_annual
        )
        -- Step 3: join both, add label
        SELECT
            r.City,
            r.rank,
            r.annual_avg_aqi,
            n.nat_avg,
            CASE
                WHEN r.annual_avg_aqi > n.nat_avg THEN 'Above Avg ⚠'
                ELSE                                   'Below Avg ✓'
            END AS vs_national
        FROM ranked r, national n
        ORDER BY r.rank
    """, conn).to_string(index=False))

    # ── Query 4: Data Quality Report ──────────────────────────────────────────
    # Every production pipeline MUST have a DQ check after load
    # In mainframe: your reconciliation reports after batch run
    print("\n" + "="*58)
    print("  DATA QUALITY REPORT")
    print("="*58)
    total       = len(raw_df)
    nulls_after = int(raw_df['AQI'].isna().sum())
    negatives   = int((raw_df['AQI'].dropna() < 0).sum())
    dq_score    = round((1 - nulls_after / total) * 100, 1)

    dq = pd.DataFrame([
        {'Check': 'Total rows',         'Value': total,        'Status': 'OK'},
        {'Check': 'Nulls before fix',   'Value': nulls_before, 'Status': 'FIXED'},
        {'Check': 'Nulls after fix',    'Value': nulls_after,  'Status': 'OK' if nulls_after == 0 else 'WARN'},
        {'Check': 'Negative AQI rows',  'Value': negatives,    'Status': 'OK' if negatives == 0 else 'FAIL'},
        {'Check': 'Duplicate rows',     'Value': dupes_before, 'Status': 'OK' if dupes_before == 0 else 'FAIL'},
        {'Check': 'DQ score (%)',       'Value': dq_score,     'Status': 'PASS' if dq_score == 100 else 'WARN'},
    ])
    print(dq.to_string(index=False))

    # ── Output 5: Pipeline run report → JSON ──────────────────────────────────
    # This JSON = your pipeline's "batch completion report"
    # Equivalent to your JES2 job completion message in mainframe
    top3 = pd.read_sql("""
        SELECT City, annual_avg_aqi
        FROM   city_annual
        ORDER  BY annual_avg_aqi DESC LIMIT 3
    """, conn).to_dict(orient='records')

    report = {
        "pipeline":    "india-aqi-etl",
        "run_date":    datetime.now().strftime("%Y-%m-%d %H:%M"),
        "status":      "SUCCESS",
        "stats": {
            "total_rows":       total,
            "cities":           int(raw_df['City'].nunique()),
            "date_range":       f"{raw_df['Date'].min()} → {raw_df['Date'].max()}",
            "nulls_fixed":      nulls_before,
            "dq_score_pct":     dq_score,
        },
        "top3_polluted": top3,
    }

    with open('pipeline_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    print("\n" + "="*58)
    print("  PIPELINE RUN REPORT  →  pipeline_report.json")
    print("="*58)
    print(json.dumps(report, indent=2))


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    t0 = datetime.now()
    logging.info("India AQI ETL Pipeline — Day 3")

    raw_df, nulls_before, dupes_before = extract('city_day.csv')
    raw_df, monthly_df                 = transform(raw_df)
    load(raw_df, monthly_df, 'air_quality.db')

    conn = sqlite3.connect('air_quality.db')
    verify_day1(conn)
    verify_day2(conn)
    verify_day3(conn, raw_df, nulls_before, dupes_before)
    conn.close()

    elapsed = (datetime.now() - t0).total_seconds()
    logging.info(f"Pipeline complete in {elapsed:.2f}s")
    logging.info("New file: pipeline_report.json")
    logging.info("Git: git add . && git commit -m 'feat: day3 LAG CTE DQ-report'")