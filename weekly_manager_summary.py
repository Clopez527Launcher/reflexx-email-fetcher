import os
import json
import datetime as dt
import mysql.connector
import requests

POSTMARK_URL = "https://api.postmarkapp.com/email"

def get_db():
    return mysql.connector.connect(
        host=os.environ["MYSQLHOST"],
        user=os.environ["MYSQLUSER"],
        password=os.environ["MYSQLPASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
    )

def postmark_send(to_email: str, subject: str, html_body: str):
    token = os.environ["POSTMARK_API_TOKEN"]
    from_email = os.environ.get("EMAIL_FROM", "no-reply@reflexxapp.com")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Postmark-Server-Token": token,
    }

    payload = {
        "From": f"ReflexxApp <{from_email}>",
        "To": to_email,
        "Subject": subject,
        "HtmlBody": html_body,
        "MessageStream": "outbound",
    }

    r = requests.post(POSTMARK_URL, headers=headers, data=json.dumps(payload), timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Postmark error {r.status_code}: {r.text}")

def week_range_label(today=None):
    # last full Monday-Sunday week ending yesterday
    if today is None:
        today = dt.date.today()
    end = today - dt.timedelta(days=1)
    start = end - dt.timedelta(days=6)
    return start, end

def build_email_html(manager_name: str, start: dt.date, end: dt.date):
    # SUPER SIMPLE V1 (we'll upgrade later with real stats)
    return f"""
    <div style="font-family: Arial, sans-serif; color:#111; line-height:1.5;">
      <h2 style="margin:0 0 10px 0;">Weekly Manager Summary</h2>
      <p style="margin:0 0 10px 0;"><b>{manager_name}</b>, here’s your weekly Reflexx summary.</p>
      <p style="margin:0 0 14px 0;">Week: <b>{start}</b> to <b>{end}</b></p>

      <p style="margin:0 0 10px 0;">
        (V1) This email is wired up ✅ — next we’ll insert:
        team index trends, top movers, bottom movers, and the biggest week-over-week changes.
      </p>

      <p style="margin:16px 0 0 0;">Happy Selling!</p>
    </div>
    """

def main():
    conn = get_db()
    cur = conn.cursor(dictionary=True)

    # pull managers that have weekly enabled
    cur.execute("""
        SELECT id, email, COALESCE(nickname, email) AS name
        FROM users
        WHERE role = 'manager'
          AND manager_summary_weekly_enabled = 1
          AND email IS NOT NULL
          AND email <> ''
    """)
    managers = cur.fetchall()

    start, end = week_range_label()

    sent = 0
    for m in managers:
        html = build_email_html(m["name"], start, end)
        subject = f"Reflexx Weekly Summary ({start} – {end})"
        postmark_send(m["email"], subject, html)
        sent += 1

    cur.close()
    conn.close()

    print(f"✅ Weekly manager summaries sent: {sent}")

if __name__ == "__main__":
    main()
