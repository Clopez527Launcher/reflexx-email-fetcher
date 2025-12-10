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

        s = s.strip()

        # Format: "0 days 00:06:30"
        if "days" in s:
            parts = s.split()
            hhmmss = parts[-1]  # last part: "00:06:30"
            t = datetime.strptime(hhmmss, "%H:%M:%S")
            return t.hour * 3600 + t.minute * 60 + t.second

        # Format: "00:06:30"
        t = datetime.strptime(s, "%H:%M:%S")
        return t.hour * 3600 + t.minute * 60 + t.second

    except Exception as e:
        print(f"⛔ Duration parsing failed: {s} → {e}")
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

    processed_elite_file = False

    for part in msg.walk():
        if part.get("Content-Disposition") and "attachment" in part.get("Content-Disposition"):
            filename = part.get_filename()

            # --- ONLY PROCESS ELITE CALLS FILE ---
            if not (filename and filename.endswith(".xlsx") and "Elite_Calls_Calls" in filename):
                print(f"⏭️ Skipping non-elite attachment: {filename}")
                continue

            processed_elite_file = True
            print(f"🔥 Processing Elite Calls file: {filename}")

            # Save attachment
            with open(filename, "wb") as f:
                f.write(part.get_payload(decode=True))
            print(f"📥 Saved: {filename}")

            # Load Excel
            try:
                df = pd.read_excel(filename, sheet_name="Calls")
                print(f"📄 Loaded {len(df)} call rows")
            except Exception as e:
                print(f"❌ Failed to read Calls sheet: {e}")
                continue

            # DB connection
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()

            inserted = 0
            MIN_SEC = 5 * 60  # 5 minutes

            for _, row in df.iterrows():

                # --- DEBUG FILTERING FOR ELITE CALLS ---
                length_raw = row.get("Call Length")
                length_sec = parse_duration_to_seconds(length_raw)

                if not length_sec:
                    print(f"❌ Skipping: no valid duration → '{length_raw}'")
                    continue

                if length_sec < MIN_SEC:
                    print(f"❌ Skipping: duration too short ({length_sec}s) → '{length_raw}'")
                    continue

                result = str(row.get("Result", "")).strip()
                if result not in ["Answered", "Connected"]:
                    print(f"❌ Skipping: result not elite → '{result}'")
                    continue

                # Determine agent name
                if result == "Answered":
                    agent_name = row.get("To Name")
                else:
                    agent_name = row.get("From Name")

                if not agent_name or str(agent_name).strip() == "":
                    print(f"❌ Skipping: missing agent name (Result={result})")
                    continue

                agent_name = str(agent_name).strip()

                # Other fields
                session_id = row.get("Session Id")
                call_start = parse_call_start(row.get("Call Start Time"))
                call_direction = str(row.get("Call Direction", "")).strip()
                queue_name = str(row.get("Queue", "")).strip()

                # SQL insert
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

    # === Mark email as SEEN only if elite file was processed ===
    if processed_elite_file:
        mail.store(num, "+FLAGS", "\\Seen")
        print("👁‍🗨 Marked email as SEEN (elite calls processed).")
    else:
        print("ℹ️ Email left unread (no elite file found).")

mail.logout()
print("🏁 Elite call import complete.")

