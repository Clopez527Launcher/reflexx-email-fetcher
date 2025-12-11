import pandas as pd
import mysql.connector
from datetime import datetime

def str_to_time_obj(t):
    try:
        return datetime.strptime(t, "%H:%M:%S").time() if isinstance(t, str) else None
    except:
        return None

# === Load Excel Sheet ===
file_path = "Daily_Report_U3_Users_04_10_2025_2_04_34_PM.xlsx"
df = pd.read_excel(file_path, sheet_name="Users", dtype=str)

# Parse User ID and Date from Filename
user_id = int(file_path.split("_")[2][1:])  # e.g., "U3" → 3
report_date = datetime.today().date()  # Or parse from filename if consistent

# === MySQL Connection ===
conn = mysql.connector.connect(
    host="autorack.proxy.rlwy.net",
    user="root",
    password="vbNVbSKVuUvYRJzhewpufAXbxcatfKIc",  # ⬅️ Put your real Railway password here
    database="railway",
    port=55185
)
cursor = conn.cursor()

# === Insert Data ===
for _, row in df.iterrows():
    try:
        extension = row["Ext"]
        total_calls = int(row["Total Calls"])
        inbound_calls = int(row["# Inbound"])
        outbound_calls = int(row["# Outbound"])
        handle_time = str_to_time_obj(row["Total Handle Time"])
        inbound_time = str_to_time_obj(row["Total Handle Time (in)"])
        outbound_time = str_to_time_obj(row["Total Handle Time (out)"])

        sql = """
        INSERT INTO call_metrics (
            user_id, report_date, extension,
            total_calls, inbound_calls, outbound_calls,
            handle_time, inbound_time, outbound_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            user_id, report_date, extension,
            total_calls, inbound_calls, outbound_calls,
            handle_time, inbound_time, outbound_time
        ))
    except Exception as e:
        print("❌ Error inserting row:", e)

conn.commit()
cursor.close()
conn.close()

print("✅ Done — data inserted into MySQL.")
