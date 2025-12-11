import os
import openai
import mysql.connector
from datetime import datetime, date, timedelta
from reportlab.lib.pagesizes import LETTER
from reportlab.platypus import Table, TableStyle, SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from pytz import timezone

# ✅ MySQL Configuration (Railway)
DB_HOST = "mysql.railway.internal"
DB_USER = "root"
DB_PASSWORD = "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc"
DB_NAME = "railway"

MYSQL_CONFIG = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": 3306
}

openai.api_key = os.getenv("OPENAI_API_KEY")
PDF_DIR = "saved_reports"
os.makedirs(PDF_DIR, exist_ok=True)

# Saved Report Query for Agency
def save_report_to_db(filename, filepath, manager_id):
    with open(filepath, "rb") as f:
        binary_data = f.read()

    conn = mysql.connector.connect(
        host="mysql.railway.internal",
        user="root",
        password="vbNVbSKVuUvYRJzhewpufAXbxcatfKIc",
        database="railway",
        port=3306
    )
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO reports (filename, file_data, manager_id) VALUES (%s, %s, %s)",
        (filename, binary_data, manager_id)
    )
    conn.commit()
    cursor.close()
    conn.close()

# === DB QUERIES ===
def fetch_metrics():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    today = date.today()
    date_str = today.strftime('%Y-%m-%d')

    # Fetch office totals from activity_log
    cursor.execute("""
        SELECT SUM(mouse_distance), SUM(keystrokes), SUM(mouse_clicks), SUM(idle_count)
        FROM activity_log WHERE DATE(timestamp) = %s
    """, (date_str,))
    total_activity = cursor.fetchone()

    # Fetch per-user activity
    cursor.execute("""
        SELECT u.email, SUM(a.mouse_distance), SUM(a.keystrokes), SUM(a.mouse_clicks), SUM(a.idle_count)
        FROM activity_log a JOIN users u ON a.user_id = u.id
        WHERE DATE(a.timestamp) = %s GROUP BY u.email
    """, (date_str,))
    user_activities = cursor.fetchall()

    # Fetch per-user call metrics
    cursor.execute("""
        SELECT u.email,
               cm.inbound_calls,
               cm.outbound_calls,
               cm.inbound_time,
               cm.outbound_time
        FROM call_metrics cm JOIN users u ON cm.user_id = u.id
        WHERE cm.report_date = %s
    """, (date_str,))
    user_calls = {row[0]: row[1:] for row in cursor.fetchall()}

    # Fetch office call totals
    cursor.execute("""
        SELECT 
            SUM(inbound_calls),
            SUM(outbound_calls),
            SEC_TO_TIME(SUM(TIME_TO_SEC(inbound_time))),
            SEC_TO_TIME(SUM(TIME_TO_SEC(outbound_time)))
        FROM call_metrics
        WHERE report_date = %s
    """, (date_str,))
    total_calls = cursor.fetchone()

    # Fetch web usage times from JSON and normalize into percentages
    cursor.execute("""
        SELECT page_time
        FROM activity_log
        WHERE DATE(timestamp) = %s
    """, (date_str,))

    from collections import defaultdict
    import json
    import decimal

    def safe_get(val):
        try:
            return float(val) if val is not None else 0
        except (decimal.InvalidOperation, ValueError, TypeError):
            return 0

    app_totals = defaultdict(float)

    # Sum all seconds per app across all users
    for (json_blob,) in cursor.fetchall():
        if json_blob:
            try:
                data = json.loads(json_blob)
                for app, seconds in data.items():
                    app_totals[app] += safe_get(seconds)
            except Exception:
                continue

    # Total time spent on all apps
    total_time = sum(app_totals.values()) or 1  # Avoid divide by zero

    # Normalize into percentages and format
    web_usage = sorted(
        [(app, f"{round((time / total_time) * 100, 2)}") for app, time in app_totals.items()],
        key=lambda x: float(x[1]),
        reverse=True
    )

    cursor.close()
    conn.close()
    return total_activity, user_activities, user_calls, total_calls, web_usage

# === AI Summary ===
def get_ai_summary(agent_data):
    prompt = """You are Reflexx AI. Here's the office performance for today:

"""
    for row in agent_data:
        name, inbound, outbound, in_talk, out_talk = row[:5]
        prompt += f"- {name}: Inbound {inbound}, Outbound {outbound}, In Talk {in_talk}, Out Talk {out_talk}\n"

    prompt += "\nPlease summarize the day's performance in one paragraph and give one improvement suggestion."

    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return response.choices[0].message.content

# === PDF Generation ===
def generate_pdf(summary, totals, agent_rows, web_usage):
    pacific = timezone("US/Pacific")
    now = datetime.now(pacific)
    timestamp = now.strftime('%b %d, %Y at %I:%M %p')
    filename = f"Reflexx Daily Report – {timestamp}.pdf"
    pdf_path = os.path.join(PDF_DIR, filename)

    doc = SimpleDocTemplate(pdf_path, pagesize=LETTER)
    styles = getSampleStyleSheet()
    elements = [
        Paragraph(f"<b>Reflexx Daily Report – {timestamp}</b>", styles['Title']),
        Spacer(1, 12),
        Paragraph(summary, styles['BodyText']),
        Spacer(1, 20)
    ]

    headers = ["Agent", "Inbound", "Outbound", "In Talk", "Out Talk", "Distance", "Keystrokes", "Clicks", "Idle"]
    table_data = [headers, ["TOTAL"] + totals] + agent_rows
    table = Table(table_data, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('BACKGROUND', (0, 1), (-1, 1), colors.lightgrey),
    ]))
    elements.extend([table, Spacer(1, 20)])

    elements.append(Paragraph("<b>Office Web Usage</b>", styles['Heading2']))
    usage_lines = "<br/>".join([f"{app}: {percent}%" for app, percent in web_usage])
    elements.append(Paragraph(usage_lines, styles['BodyText']))

    doc.build(elements)
    return pdf_path

# === Main ===
def main(manager_id):
    totals_activity, user_activities, user_calls, totals_calls, web_usage = fetch_metrics()

    # Build agent rows
    agent_rows = []
    ai_input = []
    for user in user_activities:
        name, distance, keys, clicks, idle = user
        inbound, outbound, in_talk, out_talk = user_calls.get(name, (0, 0, "0:00:00", "0:00:00"))
        row = [name, str(inbound), str(outbound), in_talk, out_talk, str(int(distance or 0)), str(int(keys or 0)), str(int(clicks or 0)), str(idle or 0)]
        agent_rows.append(row)
        ai_input.append(row)

    # Generate dynamic totals row
    total_row = [
        str(totals_calls[0] or 0),
        str(totals_calls[1] or 0),
        totals_calls[2] or "0:00:00",
        totals_calls[3] or "0:00:00",
        str(int(totals_activity[0] or 0)),
        str(int(totals_activity[1] or 0)),
        str(int(totals_activity[2] or 0)),
        str(totals_activity[3] or 0)
    ]

    summary = get_ai_summary(ai_input)
    pdf_path = generate_pdf(summary, total_row, agent_rows, web_usage)
    save_report_to_db(os.path.basename(pdf_path), pdf_path, manager_id)
    print(f"✅ Report saved for manager {manager_id}: {pdf_path}")


# Optional CLI testing:
if __name__ == "__main__":
    main(manager_id=4)  # Replace 4 with a real manager_id for manual testing
