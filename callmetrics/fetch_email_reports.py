# fetch_email_reports.py
# ------------------------------------------------------------
# Multi-agency RingCentral daily report fetcher (Gmail IMAP)
# - Finds UNSEEN emails with subject: "Scheduled Reports from RingCentral"
# - Processes any .xlsx attachment whose filename contains "Daily_Report_"
# - Extracts agency_code from filename (e.g., Daily_Report_A0D6225....xlsx)
# - Resolves manager_id via users(role='manager', agency_code=...)
# - Inserts call_metrics rows scoped to that manager (extension + manager_id)
# - Idempotent inserts using ON DUPLICATE KEY UPDATE
# ------------------------------------------------------------

import imaplib
import email
from email.header import decode_header
import os
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta

# === Credentials ===
EMAIL_USER = "chris@reflexxapp.com"
EMAIL_PASS = "ugbzskooobritkyg"  # Gmail app password, no spaces
IMAP_SERVER = "imap.gmail.com"

# === MySQL Config ===
MYSQL_CONFIG = {
    "host": "autorack.proxy.rlwy.net",
    "user": "root",
    "password": "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc",
    "database": "railway",
    "port": 55185
}

# ===== Helpers =====

def safe_decode_subject(msg):
    subject = decode_header(msg.get("Subject", ""))[0][0]
    if isinstance(subject, bytes):
        try:
            subject = subject.decode("utf-8", errors="ignore")
        except Exception:
            subject = str(subject)
    return subject or ""

def str_to_time_obj(t):
    """Convert 'HH:MM:SS' to time() or None."""
    try:
        return datetime.strptime(str(t).strip(), "%H:%M:%S").time()
    except Exception:
        return None

def parse_agency_code_from_filename(filename: str) -> str:
    """
    Extract agency code from filenames like:
      - Daily_Report_A0D6225.xlsx
      - Daily_Report_A0D6225 (1).xlsx
      - Daily_Report_A0D6225_Users.xlsx
      - Daily_Report_A0D6225 - Something.xlsx

    Strategy:
      take text after "Daily_Report_" until first "_" or " " or "."
    """
    if not filename or "Daily_Report_" not in filename:
        return ""

    after = filename.split("Daily_Report_", 1)[1]  # everything after prefix
    # stop at common separators
    for sep in ["_", " ", "."]:
        if sep in after:
            after = after.split(sep, 1)[0]
    return after.strip()

def is_daily_report_attachment(filename: str) -> bool:
    """Return True if the attachment looks like a Daily_Report_*.xlsx."""
    if not filename:
        return False
    if not filename.lower().endswith(".xlsx"):
        return False
    if "Daily_Report_" not in filename:
        return False
    return True

def get_report_date_from_email(msg) -> datetime.date:
    """
    Best-effort: use email Date header (local-ish), fallback to today.
    We keep it simple: parse with email.utils and then take date().
    """
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(msg.get("Date"))
        # If timezone-aware, convert to naive UTC then backdate safety:
        if dt.tzinfo is not None:
            dt = dt.astimezone(tz=None)
        return dt.date()
    except Exception:
        return datetime.today().date()

# ===== Main =====

def main():
    # Connect to Gmail
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_USER, EMAIL_PASS)
    mail.select("inbox")

    # Search for unread RingCentral scheduled reports
    status, messages = mail.search(None, '(UNSEEN SUBJECT "Scheduled Reports from RingCentral")')
    msg_nums = messages[0].split() if messages and messages[0] else []

    print(f"📬 Unread matches found: {len(msg_nums)}")
    print("🔥🔥🔥 HELLO FROM MULTI-AGENCY DAILY RC FETCHER 🔥🔥🔥")

    for num in msg_nums:
        processed_any_attachment = False

        try:
            _, msg_data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])

            subject = safe_decode_subject(msg)
            print(f"\n📩 Found email: {subject}")

            report_date = get_report_date_from_email(msg)
            print(f"📅 Using report_date={report_date}")

            # Walk attachments
            for part in msg.walk():
                content_disposition = part.get("Content-Disposition", "")
                if "attachment" not in content_disposition.lower():
                    continue

                filename = part.get_filename()
                if not is_daily_report_attachment(filename):
                    print(f"⏭️ Skipping attachment (not Daily_Report_*.xlsx): {filename}")
                    continue

                agency_code = parse_agency_code_from_filename(filename)
                if not agency_code:
                    print(f"⏭️ Could not parse agency_code from filename: {filename}")
                    continue

                print(f"🏷️ Attachment accepted: {filename}  |  agency_code={agency_code}")

                # Save attachment locally
                file_path = filename
                with open(file_path, "wb") as f:
                    f.write(part.get_payload(decode=True))
                print(f"📥 Saved attachment: {file_path}")

                # Open Excel
                try:
                    xl = pd.ExcelFile(file_path)
                except Exception as e:
                    print(f"⛔ Could not open Excel file {filename}: {e}")
                    continue

                # Must contain Users sheet (adjust here if RingCentral changes naming)
                if "Users" not in xl.sheet_names:
                    print(f"⏭️ Skipping attachment with no 'Users' sheet: {filename} | sheets={xl.sheet_names}")
                    continue

                df = xl.parse("Users", dtype=str)
                if df.empty:
                    print(f"ℹ️ Users sheet empty for: {filename}")
                    processed_any_attachment = True
                    continue

                # Connect DB once per attachment
                conn = mysql.connector.connect(**MYSQL_CONFIG)
                cursor = conn.cursor()

                # Resolve manager_id from agency_code (DB truth)
                cursor.execute("""
                    SELECT id
                    FROM users
                    WHERE role = 'manager'
                      AND agency_code = %s
                      AND COALESCE(is_active, 1) = 1
                    LIMIT 1
                """, (agency_code,))
                mgr = cursor.fetchone()

                if not mgr:
                    print(f"⚠️ No manager found with agency_code={agency_code}. Skipping attachment.")
                    cursor.close()
                    conn.close()
                    continue

                manager_id = mgr[0]
                print(f"👤 Resolved manager_id={manager_id} for agency_code={agency_code}")

                insert_sql = """
                    INSERT INTO call_metrics (
                        user_id, report_date, extension,
                        total_calls, inbound_calls, outbound_calls,
                        handle_time, inbound_time, outbound_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        total_calls    = VALUES(total_calls),
                        inbound_calls  = VALUES(inbound_calls),
                        outbound_calls = VALUES(outbound_calls),
                        handle_time    = VALUES(handle_time),
                        inbound_time   = VALUES(inbound_time),
                        outbound_time  = VALUES(outbound_time)
                """

                inserted = 0
                skipped_no_user = 0
                errors = 0

                for _, row in df.iterrows():
                    try:
                        ext = (row.get("Ext") or "").strip()
                        if not ext:
                            continue

                        # 🔍 Look up correct user_id by extension + manager_id
                        cursor.execute("""
                            SELECT id
                            FROM users
                            WHERE extension = %s
                              AND manager_id = %s
                              AND role = 'user'
                              AND COALESCE(is_active, 1) = 1
                            LIMIT 1
                        """, (ext, manager_id))
                        result = cursor.fetchone()

                        if not result:
                            print(f"⚠️ No ACTIVE user found for ext={ext} under manager_id={manager_id}. Skipping row.")
                            skipped_no_user += 1
                            continue

                        user_id = result[0]

                        # Parse numeric fields safely
                        def to_int(v):
                            try:
                                return int(str(v).strip())
                            except Exception:
                                return 0

                        total_calls = to_int(row.get("Total Calls"))
                        inbound_calls = to_int(row.get("# Inbound"))
                        outbound_calls = to_int(row.get("# Outbound"))

                        handle_time = str_to_time_obj(row.get("Total Handle Time"))
                        inbound_time = str_to_time_obj(row.get("Total Handle Time (in)"))
                        outbound_time = str_to_time_obj(row.get("Total Handle Time (out)"))

                        cursor.execute(insert_sql, (
                            user_id,
                            report_date,
                            ext,
                            total_calls,
                            inbound_calls,
                            outbound_calls,
                            handle_time,
                            inbound_time,
                            outbound_time
                        ))
                        inserted += 1

                    except Exception as e:
                        errors += 1
                        print(f"❌ Error inserting row for ext={row.get('Ext', 'UNKNOWN')}: {e}")

                conn.commit()
                cursor.close()
                conn.close()

                print(f"✅ Inserted/updated rows: {inserted} | skipped_no_user: {skipped_no_user} | errors: {errors}")

                processed_any_attachment = True

                # Clean up saved file (optional)
                try:
                    os.remove(file_path)
                except Exception:
                    pass

        except Exception as e:
            print(f"⛔ Error processing email {num}: {e}")

        # Mark email as seen only if we actually processed at least one valid attachment
        if processed_any_attachment:
            mail.store(num, '+FLAGS', '\\Seen')
            print("👁‍🗨 Marked email as SEEN (processed at least one attachment).")
        else:
            print("ℹ️ Did NOT mark email as SEEN (no valid attachments processed).")

    mail.logout()
    print("\n👋 Daily RC importer finished.")


if __name__ == "__main__":
    main()

