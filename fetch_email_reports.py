import imaplib
import email
from email.header import decode_header
import pandas as pd
import mysql.connector
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()  # Loads from .env locally, ignored in Railway


# === Credentials ===
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
IMAP_SERVER = os.getenv("IMAP_SERVER")

# === MySQL Config ===
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
    "port": int(os.getenv("MYSQL_PORT", 3306))
}

# === Connect to Gmail ===
mail = imaplib.IMAP4_SSL(IMAP_SERVER)
mail.login(EMAIL_USER, EMAIL_PASS)
mail.select("inbox")

# === Search for unread RingCentral reports
status, messages = mail.search(None, '(UNSEEN SUBJECT "Scheduled Reports from RingCentral")')
print(f"📬 Unread matches found: {len(messages[0].split())}")

for num in messages[0].split():
    _, msg_data = mail.fetch(num, "(RFC822)")
    msg = email.message_from_bytes(msg_data[0][1])
    subject = decode_header(msg["Subject"])[0][0]
    if isinstance(subject, bytes):
        subject = subject.decode()

    print(f"📩 Found email: {subject}")

    for part in msg.walk():
        content_disposition = part.get("Content-Disposition")
        if content_disposition and "attachment" in content_disposition:
            filename = part.get_filename()
            if filename.endswith(".xlsx"):
                file_path = filename
                with open(file_path, "wb") as f:
                    f.write(part.get_payload(decode=True))
                print(f"📥 Saved attachment: {filename}")

                # === Parse Excel & Insert
                df = pd.read_excel(file_path, sheet_name="Users", dtype=str)
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
                        print(f"❌ Error inserting row for extension {row['Ext']}: {e}")

                conn.commit()
                cursor.close()
                conn.close()
                print("✅ Data inserted into MySQL.")
                mail.store(num, '+FLAGS', '\\Seen')

mail.logout()
