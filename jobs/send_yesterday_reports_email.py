import os
from datetime import datetime, timedelta
from pytz import timezone
import mysql.connector
from email.message import EmailMessage
import smtplib

from generate_daily_report import MYSQL_CONFIG

# -----------------------------
# ✅ SMTP CONFIG (set as Railway vars)
# -----------------------------
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)

def send_email_with_pdf(to_email: str, subject: str, body: str, filename: str, pdf_bytes: bytes):
    msg = EmailMessage()
    msg["From"] = FROM_EMAIL
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=filename
    )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

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

def main():
    # ✅ sanity check
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, FROM_EMAIL]):
        print("Missing SMTP env vars: SMTP_HOST/SMTP_USER/SMTP_PASS/FROM_EMAIL")
        return

    pac = timezone("US/Pacific")
    report_date = (datetime.now(pac).date() - timedelta(days=1))
    report_date_str = report_date.strftime("%Y-%m-%d")

    conn = mysql.connector.connect(**MYSQL_CONFIG)

    managers = get_enabled_managers(conn)
    for m in managers:
        manager_id = int(m["id"])
        to_email = (m["email"] or "").strip()

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
        send_email_with_pdf(to_email, subject, body, filename, pdf_bytes)

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
