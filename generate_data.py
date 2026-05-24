"""
Week 1 — Step 0: Generate India AQI dataset
Run this first to create city_day.csv
"""
import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

cities_config = {
    'Delhi':     (180, 60), 'Mumbai':    (95, 30),
    'Bengaluru': (75, 25),  'Chennai':   (90, 28),
    'Hyderabad': (85, 27),  'Kolkata':   (140, 45),
    'Pune':      (80, 24),  'Ahmedabad': (120, 40),
    'Jaipur':    (110, 35), 'Lucknow':   (155, 50),
}

rows = []
start_date = datetime(2023, 1, 1)

for city, (mean_aqi, std_aqi) in cities_config.items():
    for i in range(365):
        date = start_date + timedelta(days=i)
        aqi  = max(10, round(random.gauss(mean_aqi, std_aqi), 1))
        rows.append({
            'City':  city,
            'Date':  date.strftime('%Y-%m-%d'),
            'AQI':   aqi if random.random() > 0.05 else None,  # 5% nulls
            'PM2.5': round(aqi * random.uniform(0.35, 0.55), 1),
            'PM10':  round(aqi * random.uniform(0.60, 0.90), 1),
            'NO2':   round(random.gauss(40, 15), 1),
        })

df = pd.DataFrame(rows)
df.to_csv('city_day.csv', index=False)
print(f"Created city_day.csv — {len(df)} rows, {df['City'].nunique()} cities")
print(f"Null AQI rows: {df['AQI'].isna().sum()}")
