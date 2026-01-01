import os
from datetime import datetime, timedelta
from pytz import timezone
import mysql.connector
import postmark

from generate_daily_report import MYSQL_CONFIG

POSTMARK_SERVER_TOKEN = os.getenv("POSTMARK_SERVER_TOKEN")
POSTMARK_FROM_EMAIL   = os.getenv("POSTMARK_FROM_EMAIL") or os.getenv("FROM_EMAIL")

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

def get_yesterday_report(conn, manager_id: int, report_date_str: str):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT filename, file_data
        FROM reports
        WHERE manager_id = %s
          AND report_date = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (manager_id, report_date_str))
    return cur.fetchone()

def send_postmark_email_with_pdf(to_email: str, subject: str, body_text: str, filename: str, pdf_bytes: bytes):
    # ✅ Postmark Python expects attachments as list[dict]
    attachment = {
        "Name": filename,
        "Content": pdf_bytes,
        "ContentType": "application/pdf"
    }

    pm = postmark.PMMail(
        api_key=POSTMARK_SERVER_TOKEN,
        sender=POSTMARK_FROM_EMAIL,
        to=to_email,
        subject=subject,
        text_body=body_text,
        attachments=[attachment],
    )
    pm.send()

def main():
    if not POSTMARK_SERVER_TOKEN:
        print("Missing env var: POSTMARK_SERVER_TOKEN")
        return
    if not POSTMARK_FROM_EMAIL:
        print("Missing env var: POSTMARK_FROM_EMAIL (or FROM_EMAIL)")
        return

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
        to_email = (m["email"] or "").strip()
        if not to_email:
            print(f"[Email] manager_id={manager_id} has no email, skipping")
            continue

        row = get_yesterday_report(conn, manager_id, report_date_str)
        if not row:
            print(f"[Email] No report found for manager_id={manager_id} date={report_date_str}")
            continue

        filename = row["filename"]
        pdf_bytes = row["file_data"]

        subject = f"Reflexx — Yesterday Summary ({report_date_str})"
        body = (
            f"Attached is your Reflexx Yesterday Summary for {report_date_str}.\n\n"
            f"— Reflexx"
        )

        print(f"[Email] Sending to {to_email} manager_id={manager_id} date={report_date_str}")
        send_postmark_email_with_pdf(to_email, subject, body, filename, pdf_bytes)

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
