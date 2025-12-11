import os
import pandas as pd
import mysql.connector

# ---------------------------
# DB CONFIG (PUBLIC HOST)
# ---------------------------
DB_HOST = "autorack.proxy.rlwy.net"
DB_PORT = 55185
DB_NAME = "railway"
DB_USER = "root"
DB_PASS = "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc"

conn = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME
)

sql = """
SELECT
  date,
  user_id,

  -- PHONE
  inbounds,
  outbounds,
  ib_time_minutes,
  ob_time_minutes,

  -- MOVEMENT
  idle_time_seconds,
  keystrokes,
  mouse_clicks,
  mouse_distance,

  -- QUOTING
  quotes_unique,
  quoted_items,
  advisor_pro_minutes

FROM fact_daily
WHERE date >= CURDATE() - INTERVAL 45 DAY
  AND NOT (
    IFNULL(inbounds,0) = 0
    AND IFNULL(outbounds,0) = 0
    AND IFNULL(ib_time_minutes,0) = 0
    AND IFNULL(ob_time_minutes,0) = 0
  )
ORDER BY date DESC, user_id;
"""

df = pd.read_sql(sql, conn)
conn.close()

print(f"Pulled {len(df)} rows")

out_path = "buckets_last_45_days.csv"
df.to_csv(out_path, index=False)

print(f"✅ CSV saved to: {out_path}")
