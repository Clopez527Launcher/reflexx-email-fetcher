import imaplib
import email
from email.header import decode_header
import pandas as pd
import mysql.connector
from datetime import datetime

# === Email Credentials ===
EMAIL_USER = "chris@reflexxapp.com"
EMAIL_PASS = "ugbzskooobritkyg"
IMAP_SERVER = "imap.gmail.com"

# === MySQL Config ===
MYSQL_CONFIG = {
    "host": "autorack.proxy.rlwy.net",
    "user": "root",
    "password": "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc",
    "database": "railway",
    "port": 55185
}

# --- Helpers ---
def parse_duration_to_seconds(s):
    try:
        if not isinstance(s, str):
            return None
        t = datetime.strptime(s, "%H:%M:%S")
        return t.hour * 3600 + t.minute * 60 + t.second
    except:
        return None

def parse_call_start(raw):
    try:
        if not isinstance(raw, str):
            return None
        return datetime.strptime(raw, "%m/%d/%Y %I:%M:%S %p")
    except:
        return None

# === Connect to Gmail ===
mail = imaplib.IMAP4_SSL(IMAP_SERVER)
mail.login(EMAIL_USER, EMAIL_PASS)
mail.select("inbox")

# === Search for unread RingCentral reports ===
status, messages = mail.search(None, '(UNSEEN SUBJECT "Scheduled Reports from RingCentral")')
print(f"📬 Unread matches: {len(messages[0].split())}")

for num in messages[0].split():
    _, msg_data = mail.fetch(num, "(RFC822)")
    msg = email.message_from_bytes(msg_data[0][1])
    subject = decode_header(msg["Subject"])[0][0]
    if isinstance(subject, bytes):
        subject = subject.decode()

    print(f"📩 Found email: {subject}")

    for part in msg.walk():
        if part.get("Content-Disposition") and "attachment" in part.get("Content-Disposition"):
            filename = part.get_filename()
            if filename and filename.endswith(".xlsx"):

                # Save the attachment
                with open(filename, "wb") as f:
                    f.write(part.get_payload(decode=True))
                print(f"📥 Saved: {filename}")

                # === Load CALLS sheet ===
                df = pd.read_excel(filename, sheet_name="Calls")
                print(f"📄 Loaded {len(df)} call rows")

                # === Connect to database ===
                conn = mysql.connector.connect(**MYSQL_CONFIG)
                cursor = conn.cursor()

                inserted = 0
                MIN_SEC = 5 * 60  # 5 minutes

                for _, row in df.iterrows():

                    # --- Filter elite calls ---
                    length_raw = row.get("Call Length")
                    length_sec = parse_duration_to_seconds(length_raw)

                    if not length_sec or length_sec < MIN_SEC:
                        continue  # not elite

                    result = str(row.get("Result", "")).strip()

                    # Determine agent name based on Result
                    if result == "Answered":
                        agent_name = row.get("To Name")
                    elif result == "Connected":
                        agent_name = row.get("From Name")
                    else:
                        continue  # only elite if Answered or Connected

                    if not isinstance(agent_name, str) or agent_name.strip() == "":
                        continue

                    agent_name = agent_name.strip()

                    session_id = row.get("Session Id")
                    call_start = parse_call_start(row.get("Call Start Time"))
                    call_direction = str(row.get("Call Direction", "")).strip()
                    queue_name = str(row.get("Queue", "")).strip()

                    sql = """
                    INSERT INTO ring_central_elite_calls (
                        session_id,
                        agent_name,
                        result,
                        call_length_seconds,
                        call_start,
                        call_direction,
                        queue_name
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """

                    try:
                        cursor.execute(sql, (
                            int(session_id) if session_id else None,
                            agent_name,
                            result,
                            length_sec,
                            call_start,
                            call_direction,
                            queue_name
                        ))
                        inserted += 1
                    except Exception as e:
                        print(f"❌ Insert error: {e}")

                conn.commit()
                cursor.close()
                conn.close()

                print(f"✅ Inserted {inserted} elite calls.")

                # Mark email as read
                mail.store(num, "+FLAGS", "\\Seen")

mail.logout()
print("🏁 Elite call import complete.")
