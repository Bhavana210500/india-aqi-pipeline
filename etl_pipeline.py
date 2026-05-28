"""
Week 1 — India Air Quality ETL Pipeline
Day 4: PIVOT, NTILE, Self JOIN, pandas merge, multi-format export

Stack : pandas + SQLite
Run   : python etl_pipeline.py

Day 1: extract → transform → load → basic queries
Day 2: seasonal analysis, RANK, ROW_NUMBER window functions, JSON export
Day 3: LAG/LEAD, rolling avg, CTE, data quality report, pipeline report JSON
Day 4: PIVOT (CASE WHEN), NTILE quartiles, Self JOIN, pandas merge, Parquet export
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
def extract(filepath: str):
    logging.info(f"EXTRACT  reading {filepath}")
    df = pd.read_csv(filepath, parse_dates=['Date'])
    nulls = int(df['AQI'].isna().sum())
    dupes = int(df.duplicated(subset=['City', 'Date']).sum())
    logging.info(f"EXTRACT  shape={df.shape}  nulls={nulls}  dupes={dupes}")
    return df, nulls, dupes


# ── TRANSFORM ─────────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame):
    logging.info("TRANSFORM  starting")

    df['AQI'] = df['AQI'].fillna(
        df.groupby('City')['AQI'].transform('median')
    )

    def aqi_category(aqi):
        if   aqi <= 50:  return 'Good'
        elif aqi <= 100: return 'Satisfactory'
        elif aqi <= 200: return 'Moderate'
        elif aqi <= 300: return 'Poor'
        else:            return 'Severe'

    df['AQI_Category'] = df['AQI'].apply(aqi_category)
    df['Month']  = df['Date'].dt.month
    df['Year']   = df['Date'].dt.year
    df['Season'] = df['Month'].map({
        12:'Winter', 1:'Winter',  2:'Winter',
         3:'Spring', 4:'Spring',  5:'Spring',
         6:'Monsoon',7:'Monsoon', 8:'Monsoon',
         9:'Post-Monsoon',10:'Post-Monsoon',11:'Post-Monsoon',
    })

    monthly = (
        df.groupby(['City','Year','Month','Season'])
          .agg(avg_aqi=('AQI','mean'), max_aqi=('AQI','max'),
               min_aqi=('AQI','min'),
               days_poor=('AQI', lambda x:(x>200).sum()),
               avg_pm25=('PM2.5','mean'))
          .round(2).reset_index()
    )
    monthly = monthly.sort_values(['City','Month'])
    monthly['rolling_3m_aqi'] = (
        monthly.groupby('City')['avg_aqi']
               .transform(lambda x: x.rolling(3, min_periods=1).mean())
               .round(2)
    )
    logging.info(f"TRANSFORM  monthly rows={len(monthly)}")
    return df, monthly


# ── LOAD ──────────────────────────────────────────────────────────────────────
def load(raw_df, monthly_df, db_path):
    logging.info(f"LOAD  writing to {db_path}")
    conn = sqlite3.connect(db_path)
    raw_df.to_sql('raw_aqi',      conn, if_exists='replace', index=False)
    monthly_df.to_sql('monthly_aqi', conn, if_exists='replace', index=False)
    conn.execute("DROP VIEW IF EXISTS city_annual")
    conn.execute("""
        CREATE VIEW city_annual AS
        SELECT City, Year,
               ROUND(AVG(avg_aqi),1) AS annual_avg_aqi,
               SUM(days_poor)        AS total_poor_days
        FROM   monthly_aqi GROUP BY City, Year
    """)
    conn.commit()
    conn.close()
    logging.info(f"LOAD  raw_aqi({len(raw_df)}) monthly_aqi({len(monthly_df)})")


# ── VERIFY DAYS 1-3 (condensed) ───────────────────────────────────────────────
def verify_days_1_to_3(conn):
    print("\n" + "="*60)
    print("  DAYS 1-3 — QUICK RECAP")
    print("="*60)
    print(pd.read_sql("""
        SELECT City, annual_avg_aqi, total_poor_days
        FROM   city_annual ORDER BY annual_avg_aqi DESC
    """, conn).to_string(index=False))


# ── VERIFY DAY 4 — NEW ────────────────────────────────────────────────────────
def verify_day4(conn, raw_df):

    # ── Query 1: PIVOT using CASE WHEN ───────────────────────────────────────
    # Pivot = rotate rows into columns. Makes data human-readable.
    # Real use: monthly/seasonal dashboards, Excel-style reports from SQL
    # Mainframe parallel: COBOL EVALUATE WHEN → branch by season value
    print("\n" + "="*60)
    print("  PIVOT — AQI BY SEASON PER CITY  (CASE WHEN)")
    print("  Pattern: AVG(CASE WHEN col='val' THEN metric END)")
    print("="*60)
    print(pd.read_sql("""
        SELECT
            City,
            ROUND(AVG(CASE WHEN Season='Winter'       THEN avg_aqi END),1) AS Winter,
            ROUND(AVG(CASE WHEN Season='Spring'       THEN avg_aqi END),1) AS Spring,
            ROUND(AVG(CASE WHEN Season='Monsoon'      THEN avg_aqi END),1) AS Monsoon,
            ROUND(AVG(CASE WHEN Season='Post-Monsoon' THEN avg_aqi END),1) AS Post_Monsoon
        FROM  monthly_aqi
        GROUP BY City
        ORDER BY Winter DESC
    """, conn).to_string(index=False))
    # Read: Delhi Winter=185 is worst. Bengaluru stays flat all year (our city!)
    # Interview: "Pivot a table without using PIVOT keyword" — this is the answer

    # ── Query 2: NTILE — divide into quartiles ────────────────────────────────
    # NTILE(4) splits rows into 4 equal buckets (quartiles)
    # Q1=cleanest, Q4=most polluted
    # Real use: tiering customers, SLA buckets, performance bands
    print("\n" + "="*60)
    print("  NTILE — POLLUTION QUARTILE BUCKETS")
    print("  Q1=cleanest  Q4=most polluted")
    print("="*60)
    print(pd.read_sql("""
        SELECT
            City,
            annual_avg_aqi,
            NTILE(4) OVER (ORDER BY annual_avg_aqi)  AS quartile,
            CASE NTILE(4) OVER (ORDER BY annual_avg_aqi)
                WHEN 1 THEN 'Clean'
                WHEN 2 THEN 'Moderate'
                WHEN 3 THEN 'Polluted'
                WHEN 4 THEN 'Severe'
            END AS tier
        FROM city_annual
        ORDER BY annual_avg_aqi
    """, conn).to_string(index=False))

    # ── Query 3: SELF JOIN — compare every city to Delhi as baseline ──────────
    # JOIN a table to ITSELF using two aliases (a and b)
    # Use case: compare every row against a reference row (benchmark)
    # Mainframe parallel: reading same VSAM file twice with diff keys
    print("\n" + "="*60)
    print("  SELF JOIN — EVERY CITY vs DELHI BASELINE")
    print("  Pattern: FROM table a JOIN table b ON b.City='Delhi'")
    print("="*60)
    print(pd.read_sql("""
        SELECT
            a.City,
            a.annual_avg_aqi                                          AS city_aqi,
            b.annual_avg_aqi                                          AS delhi_aqi,
            ROUND(((a.annual_avg_aqi - b.annual_avg_aqi)
                    / b.annual_avg_aqi) * 100, 1)                     AS pct_vs_delhi
        FROM  city_annual a
        JOIN  city_annual b ON b.City = 'Delhi'
        WHERE a.City != 'Delhi'
        ORDER BY pct_vs_delhi
    """, conn).to_string(index=False))
    # Bengaluru = -59.6% vs Delhi — you breathe air 60% cleaner than Delhi!

    # ── Feature 4: pandas .merge() — Python-side JOIN ─────────────────────────
    # merge() is SQL JOIN done in pandas — same logic, different syntax
    # how='left'  = LEFT JOIN  (keep all rows from left df)
    # how='inner' = INNER JOIN (only matching rows)
    # how='outer' = FULL OUTER JOIN
    print("\n" + "="*60)
    print("  pandas MERGE — monthly + annual  (LEFT JOIN in Python)")
    print("  Shows months where Bengaluru was ABOVE its own annual avg")
    print("="*60)
    monthly_df = pd.read_sql("SELECT City, Month, Season, avg_aqi FROM monthly_aqi", conn)
    annual_df  = pd.read_sql("SELECT City, annual_avg_aqi FROM city_annual", conn)

    merged = monthly_df.merge(annual_df, on='City', how='left')
    merged['above_annual'] = (merged['avg_aqi'] > merged['annual_avg_aqi'])

    blr = merged[merged['City'] == 'Bengaluru'].copy()
    blr['status'] = blr['above_annual'].map({True: 'Above ↑', False: 'Below ↓'})
    print(blr[['City','Month','Season','avg_aqi','annual_avg_aqi','status']]
            .to_string(index=False))

    # ── Feature 5: Multi-format export ───────────────────────────────────────
    # CSV   = universal, human-readable, every tool reads it
    # JSON  = APIs, web dashboards, microservices
    # Parquet = columnar, compressed, used in BigQuery/Spark/data lakes
    #           You WILL use Parquet from Week 4 onwards
    print("\n" + "="*60)
    print("  MULTI-FORMAT EXPORT")
    print("="*60)
    summary = pd.read_sql("SELECT * FROM city_annual", conn)

    summary.to_csv('city_annual.csv', index=False)
    print("CSV     → city_annual.csv      ✓  (Excel/Sheets readable)")

    summary.to_json('city_annual.json', orient='records', indent=2)
    print("JSON    → city_annual.json     ✓  (API/dashboard ready)")

    # Parquet — install once: pip install pyarrow
    try:
        summary.to_parquet('city_annual.parquet', index=False)
        print("Parquet → city_annual.parquet  ✓  (BigQuery/Spark native)")
    except ImportError:
        print("Parquet → run: pip install pyarrow  (needed from Week 4)")

    # ── Feature 6: pandas pivot_table — same as SQL PIVOT, pure Python ────────
    print("\n" + "="*60)
    print("  pandas pivot_table — AQI HEATMAP BY CITY + SEASON")
    print("="*60)
    monthly_full = pd.read_sql("SELECT City, Season, avg_aqi FROM monthly_aqi", conn)
    pivot = monthly_full.pivot_table(
        index='City', columns='Season', values='avg_aqi',
        aggfunc='mean'
    ).round(1)
    print(pivot.to_string())
    # This is the Python equivalent of your SQL CASE WHEN pivot above
    # pivot_table() is used to prep data for charts and dashboards

    # ── Feature 7: Final pipeline report update ───────────────────────────────
    report = {
        "pipeline":  "india-aqi-etl",
        "day":       4,
        "run_date":  datetime.now().strftime("%Y-%m-%d %H:%M"),
        "status":    "SUCCESS",
        "new_skills": ["PIVOT (CASE WHEN)", "NTILE", "Self JOIN",
                       "pandas merge", "multi-format export",
                       "pivot_table"],
        "exports":   ["city_annual.csv", "city_annual.json",
                      "city_annual.parquet (if pyarrow installed)"],
        "days_completed": 4,
        "week1_progress": "4/5 days done"
    }
    with open('pipeline_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    print("\n" + "="*60)
    print("  PIPELINE REPORT updated → pipeline_report.json")
    print("="*60)
    print(json.dumps(report, indent=2))


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    t0 = datetime.now()
    logging.info("India AQI ETL Pipeline — Day 4")

    raw_df, nulls_before, dupes_before = extract('city_day.csv')
    raw_df, monthly_df                 = transform(raw_df)
    load(raw_df, monthly_df, 'air_quality.db')

    conn = sqlite3.connect('air_quality.db')
    verify_days_1_to_3(conn)
    verify_day4(conn, raw_df)
    conn.close()

    elapsed = (datetime.now() - t0).total_seconds()
    logging.info(f"Pipeline complete in {elapsed:.2f}s")
    logging.info("New files: city_annual.csv  city_annual.json  pipeline_report.json")
    logging.info("Day 5 tomorrow: wrap everything into reusable functions + GitHub cleanup")