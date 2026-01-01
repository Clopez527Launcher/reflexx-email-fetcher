import os
import sys
import traceback
from datetime import datetime, timedelta

# ✅ Ensure project root is importable when running from /app/jobs
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from pytz import timezone
import mysql.connector

# ✅ import your generator + DB config
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

    print("[Generate] Starting job", flush=True)
    print("[Generate] report_date_str =", report_date_str, flush=True)

    conn = mysql.connector.connect(**MYSQL_CONFIG)

    try:
        managers = get_enabled_managers(conn)
        if not managers:
            print("[Generate] No managers have manager_summary_daily_enabled=1", flush=True)
            return

        for m in managers:
            manager_id = int(m["id"])
            print(f"[Generate] manager_id={manager_id} report_date={report_date_str}", flush=True)

            try:
                filename, pdf_bytes = generate_report_main(manager_id)
            except Exception:
                print(f"[Generate] FAILED manager_id={manager_id}", flush=True)
                traceback.print_exc()
                sys.stdout.flush()
                continue

            try:
                upsert_report(conn, manager_id, report_date_str, filename, pdf_bytes)
                print(f"[Generate] Saved report manager_id={manager_id} date={report_date_str}", flush=True)
            except Exception:
                print(f"[Generate] DB SAVE FAILED manager_id={manager_id} date={report_date_str}", flush=True)
                traceback.print_exc()
                sys.stdout.flush()
                continue

    finally:
        try:
            conn.close()
        except Exception:
            pass

    print("[Generate] Done.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("[Generate] FATAL ERROR", flush=True)
        traceback.print_exc()
        sys.stdout.flush()
        raise
