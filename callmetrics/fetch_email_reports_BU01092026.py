import imaplib
import email
from email.header import decode_header
import os
import pandas as pd
import mysql.connector
from datetime import datetime

# === Credentials ===
EMAIL_USER = "chris@reflexxapp.com"
EMAIL_PASS = "ugbzskooobritkyg"  # Your app password, no spaces
IMAP_SERVER = "imap.gmail.com"

# === MySQL Config ===
MYSQL_CONFIG = {
    "host": "autorack.proxy.rlwy.net",
    "user": "root",
    "password": "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc",
    "database": "railway",
    "port": 55185
}

# === Connect to Gmail ===
mail = imaplib.IMAP4_SSL(IMAP_SERVER)
mail.login(EMAIL_USER, EMAIL_PASS)
mail.select("inbox")

# === Search for unread RingCentral reports 
status, messages = mail.search(None, '(UNSEEN SUBJECT "Scheduled Reports from RingCentral")')
print(f"📬 Unread matches found: {len(messages[0].split())}")
print("🔥🔥🔥 HELLO FROM NEW DAILY SCRIPT 🔥🔥🔥")

for num in messages[0].split():
    _, msg_data = mail.fetch(num, "(RFC822)")
    msg = email.message_from_bytes(msg_data[0][1])

    subject = decode_header(msg["Subject"])[0][0]
    if isinstance(subject, bytes):
        subject = subject.decode()

    print(f"📩 Found email: {subject}")

    # We will ONLY process attachments that are the Daily_Report_U3_Users file
    processed_any_users_file = False

    for part in msg.walk():
        content_disposition = part.get("Content-Disposition")
        if content_disposition and "attachment" in content_disposition:
            filename = part.get_filename()

            # 1) Must be an .xlsx file
            if not filename or not filename.endswith(".xlsx"):
                print(f"⏭️ Skipping non-xlsx attachment: {filename}")
                continue

            # 2) Must have the correct daily Users filename pattern
            if "Daily_Report_U3_Users" not in filename:
                print(f"⏭️ Skipping attachment (name doesn't match Daily_Report_U3_Users): {filename}")
                continue

            # ✅ At this point, we *like* the filename, so we save it
            file_path = filename
            with open(file_path, "wb") as f:
                f.write(part.get_payload(decode=True))
            print(f"📥 [DAILY SCRIPT] Saved attachment (daily users): {filename}")

            # 3) Extra safety: make sure it actually has a 'Users' sheet
            try:
                xl = pd.ExcelFile(file_path)
            except Exception as e:
                print(f"⛔ Could not open Excel file {filename}: {e}")
                continue

            if "Users" not in xl.sheet_names:
                print(f"⏭️ Skipping attachment with no 'Users' sheet: {filename}")
                continue

            # Now safely read the Users sheet
            df = xl.parse("Users", dtype=str)

            report_date = datetime.today().date()

            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()


            def str_to_time_obj(t):
                try:
                    return datetime.strptime(t, "%H:%M:%S").time()
                except:
                    return None

            for _, row in df.iterrows():
                try:
                    ext = row["Ext"]

                    # 🔍 Look up the correct user_id based on extension
                    cursor.execute("SELECT id FROM users WHERE extension = %s", (ext,))
                    result = cursor.fetchone()

                    if not result:
                        print(f"⚠️ No user found for extension {ext}. Skipping.")
                        continue

                    user_id = result[0]  # ✅ Correct user for this row

                    sql = """
                    INSERT INTO call_metrics (
                        user_id, report_date, extension,
                        total_calls, inbound_calls, outbound_calls,
                        handle_time, inbound_time, outbound_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        user_id,
                        report_date,
                        ext,
                        int(row["Total Calls"]),
                        int(row["# Inbound"]),
                        int(row["# Outbound"]),
                        str_to_time_obj(row["Total Handle Time"]),
                        str_to_time_obj(row["Total Handle Time (in)"]),
                        str_to_time_obj(row["Total Handle Time (out)"])
                    ))

                except Exception as e:
                    print(f"❌ Error inserting row for extension {row.get('Ext', 'UNKNOWN')}: {e}")

            conn.commit()
            cursor.close()
            conn.close()
            print("✅ Daily Users data inserted into MySQL.")

            processed_any_users_file = True

    # Only mark the email as seen if we actually processed the Users file
    if processed_any_users_file:
        mail.store(num, '+FLAGS', '\\Seen')
        print("👁‍🗨 Marked email as SEEN (daily users processed).")
    else:
        print("ℹ️ Did NOT mark email as SEEN (no Users file processed).")

mail.logout()
print("👋 Daily RC importer finished.")
