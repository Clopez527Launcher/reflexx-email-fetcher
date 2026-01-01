import os
from datetime import datetime, timedelta
from pytz import timezone
import mysql.connector

# ✅ import your generator
from generate_daily_report import main as generate_report_main, MYSQL_CONFIG

def get_enabled_managers(conn):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, email
        FROM users
        WHERE role = 'manager'
          AND is_active = 1
          AND manager_summary_daily_enabled = 1
    """)
    return cur.fetchall()

def upsert_report(conn, manager_id: int, report_date_str: str, filename: str, pdf_bytes: bytes):
    cur = conn.cursor()

    # ✅ If there's already a report for that manager+date, overwrite it
    # We'll do it by: delete then insert (simple + reliable)
    cur.execute("""
        DELETE FROM reports
        WHERE manager_id = %s
          AND report_date = %s
    """, (manager_id, report_date_str))

    cur.execute("""
        INSERT INTO reports (filename, file_data, created_at, manager_id, report_date)
        VALUES (%s, %s, NOW(), %s, %s)
    """, (filename, pdf_bytes, manager_id, report_date_str))

    conn.commit()

def main():
    pac = timezone("US/Pacific")
    report_date = (datetime.now(pac).date() - timedelta(days=1))
    report_date_str = report_date.strftime("%Y-%m-%d")

    conn = mysql.connector.connect(**MYSQL_CONFIG)

    managers = get_enabled_managers(conn)
    if not managers:
        print("No managers have manager_summary_daily_enabled=1")
        conn.close()
        return


    for m in managers:
        manager_id = int(m["id"])
        print(f"[Generate] manager_id={manager_id} report_date={report_date_str}")

        try:
            filename, pdf_bytes = generate_report_main(manager_id)
        except Exception as e:
            print(f"[Generate] FAILED manager_id={manager_id} err={e}")
            continue


        upsert_report(conn, manager_id, report_date_str, filename, pdf_bytes)

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
