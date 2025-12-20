import os
import json
import requests
import mysql.connector
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Los_Angeles")

# =========================
# CONFIG (Railway Variables)
# =========================
POSTMARK_API_TOKEN = os.getenv("POSTMARK_API_TOKEN", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", "no-reply@reflexxapp.com")

# If you already use MYSQL_URL, keep it.
MYSQL_URL = os.getenv("MYSQL_URL") or os.getenv("DATABASE_URL")  # optional
MYSQLHOST = os.getenv("MYSQLHOST")
MYSQLUSER = os.getenv("MYSQLUSER")
MYSQLPASSWORD = os.getenv("MYSQLPASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # optional

# =========================
# DB Helpers
# =========================
def get_db_connection():
    # If you already have a shared helper in your app, mirror that.
    # This version uses discrete vars because that’s what Railway shows in your screenshot.
    return mysql.connector.connect(
        host=MYSQLHOST,
        user=MYSQLUSER,
        password=MYSQLPASSWORD,
        database=MYSQL_DATABASE,
        autocommit=False,
    )

def pacific_yesterday():
    now_pt = datetime.now(PACIFIC)
    y = (now_pt - timedelta(days=1)).date()
    return y

def date_range_last_7_days_ending_yesterday():
    end_day = pacific_yesterday()
    start_day = end_day - timedelta(days=6)  # 7 days total
    return start_day, end_day

# =========================
# Data pulls (EDIT TABLE/COLS IF NEEDED)
# =========================
def get_enabled_managers(conn):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, email, nickname
        FROM users
        WHERE role='manager'
          AND manager_summary_weekly_enabled=1
    """)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_team_users(conn, manager_id):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, email, nickname
        FROM users
        WHERE role='user'
          AND manager_id=%s
        ORDER BY nickname ASC
    """, (manager_id,))
    rows = cur.fetchall()
    cur.close()
    return rows

def pull_team_metrics_l7(conn, manager_id, start_day, end_day):
    """
    IMPORTANT:
    You might store these metrics in different tables.
    Update the FROM/JOIN below to match your real schema.

    Strategy:
    - pull per-user rollups across last 7 days
    - keep it defensive with COALESCE
    """

    cur = conn.cursor(dictionary=True)

    # ---- Example: if you have a fact_daily table with talk_seconds, idle_seconds, etc.
    # If your actual table is named differently, swap it here.
    cur.execute("""
        SELECT
            u.id AS user_id,
            u.nickname AS name,

            -- Talk time
            COALESCE(SUM(fd.talk_seconds), 0) AS talk_seconds,

            -- Idle time
            COALESCE(SUM(fd.idle_seconds), 0) AS idle_seconds,

            -- Movement
            COALESCE(SUM(fd.keystrokes), 0) AS keystrokes,
            COALESCE(SUM(fd.mouse_clicks), 0) AS mouse_clicks,

            -- Quotes (rename if needed)
            COALESCE(SUM(fd.quotes_count), 0) AS quotes_count,

            -- Index (daily)
            COALESCE(AVG(fd.adjusted_index), NULL) AS avg_adjusted_index

        FROM users u
        LEFT JOIN fact_daily fd
               ON fd.user_id = u.id
              AND fd.day >= %s
              AND fd.day <= %s
        WHERE u.role='user'
          AND u.manager_id=%s
        GROUP BY u.id, u.nickname
        ORDER BY u.nickname ASC
    """, (str(start_day), str(end_day), manager_id))

    rows = cur.fetchall()
    cur.close()
    return rows

def pull_team_index_l7(conn, manager_id, start_day, end_day):
    """
    Team index trend/average across L-7.
    Adjust table/column names to your setup.

    If you store team index in totals_activity or a team_daily table, point this there.
    """
    cur = conn.cursor(dictionary=True)

    # Try a totals_activity style table first (edit if your name differs)
    cur.execute("""
        SELECT
          day,
          team_adjusted_index
        FROM totals_activity
        WHERE manager_id=%s
          AND day >= %s
          AND day <= %s
        ORDER BY day ASC
    """, (manager_id, str(start_day), str(end_day)))

    rows = cur.fetchall()
    cur.close()

    # Compute average if we got anything
    vals = [r["team_adjusted_index"] for r in rows if r.get("team_adjusted_index") is not None]
    avg_val = round(sum(vals) / len(vals), 2) if vals else None

    return rows, avg_val

# =========================
# Message building
# =========================
def minutes(sec):
    try:
        return round((sec or 0) / 60.0)
    except:
        return 0

def build_summary_text(manager_name, start_day, end_day, team_index_avg, team_rows, rep_rows):
    lines = []
    lines.append("Weekly Manager Summary")
    lines.append("")
    lines.append(f"{manager_name}, here’s your weekly Reflexx summary.")
    lines.append("")
    lines.append(f"Week: {start_day} to {end_day}")
    lines.append("")

    # ---- Office Summary (no tables)
    lines.append("Office Summary (last week)")
    if team_index_avg is not None:
        lines.append(f"- Team Adjusted Index (L-7 average): {team_index_avg}")
    else:
        lines.append("- Team Adjusted Index (L-7 average): (not available yet)")

    # Simple “what happened” signals
    # (You can refine these once you like the feel.)
    if rep_rows:
        # Find top/bottom talk, idle
        talk_sorted = sorted(rep_rows, key=lambda r: (r.get("talk_seconds") or 0), reverse=True)
        idle_sorted = sorted(rep_rows, key=lambda r: (r.get("idle_seconds") or 0), reverse=True)

        top_talk = talk_sorted[0]
        top_idle = idle_sorted[0]

        lines.append(f"- Highest talk time: {top_talk.get('name')} ({minutes(top_talk.get('talk_seconds'))} mins)")
        lines.append(f"- Highest idle time: {top_idle.get('name')} ({minutes(top_idle.get('idle_seconds'))} mins)")
    lines.append("")

    # ---- Coaching Suggestions (this week)
    lines.append("Coaching Suggestions (this week)")
    for r in rep_rows:
        name = r.get("name") or "Rep"
        talk_m = minutes(r.get("talk_seconds"))
        idle_m = minutes(r.get("idle_seconds"))
        quotes = int(r.get("quotes_count") or 0)

        # Basic rule-based coaching (fast + consistent).
        # Later, we can replace/augment with AI once you like the structure.
        bullets = []

        if idle_m >= 120:
            bullets.append("Reduce idle blocks — set a 30/30 rhythm (30 mins calling, 30 mins quoting/admin).")
        elif idle_m >= 90:
            bullets.append("Idle is a bit high — tighten transitions between tasks and stay in a single workflow.")
        else:
            bullets.append("Idle looks controlled — keep your workflow tight.")

        if talk_m < 120:
            bullets.append("Push talk time up — aim for a stronger daily call block and fewer short gaps.")
        else:
            bullets.append("Talk time is healthy — keep the volume consistent.")

        if quotes < 10:
            bullets.append("Quoting volume is light — set a minimum daily quote target and track it.")
        else:
            bullets.append("Quoting volume is solid — focus on quality + speed to bind more.")

        lines.append(f"{name}:")
        for b in bullets[:3]:
            lines.append(f"- {b}")
        lines.append("")

    lines.append("Happy Selling!")
    return "\n".join(lines)

# =========================
# Optional AI polish (keeps same content, nicer wording)
# =========================
def ai_polish(text):
    if not OPENAI_API_KEY:
        return text

    try:
        # If you already have an OpenAI helper elsewhere, use that instead.
        # Keeping this simple and non-fancy.
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4.1-mini",
                "messages": [
                    {"role": "system", "content": "Rewrite for a manager weekly coaching email. Keep it short, punchy, and actionable. No tables. Preserve names, numbers, and structure."},
                    {"role": "user", "content": text},
                ],
                "temperature": 0.4,
            },
            timeout=30,
        )
        data = resp.json()
        out = data["choices"][0]["message"]["content"]
        return out.strip() if out else text
    except:
        return text

# =========================
# Postmark send
# =========================
def send_postmark_email(to_email, subject, body_text):
    if not POSTMARK_API_TOKEN:
        raise RuntimeError("Missing POSTMARK_API_TOKEN")

    payload = {
        "From": f"ReflexxApp <{FROM_EMAIL}>",
        "To": to_email,
        "Subject": subject,
        "TextBody": body_text,
        "MessageStream": "outbound",  # if you use a different stream name, change it
    }

    r = requests.post(
        "https://api.postmarkapp.com/email",
        headers={
            "X-Postmark-Server-Token": POSTMARK_API_TOKEN,
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=30,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Postmark send failed {r.status_code}: {r.text}")

# =========================
# MAIN
# =========================
def main():
    start_day, end_day = date_range_last_7_days_ending_yesterday()

    conn = get_db_connection()
    try:
        managers = get_enabled_managers(conn)

        for m in managers:
            manager_id = m["id"]
            manager_email = m["email"]
            manager_name = m.get("nickname") or "Manager"

            team_rows, team_index_avg = pull_team_index_l7(conn, manager_id, start_day, end_day)
            rep_rows = pull_team_metrics_l7(conn, manager_id, start_day, end_day)

            body = build_summary_text(manager_name, start_day, end_day, team_index_avg, team_rows, rep_rows)
            body = ai_polish(body)

            subject = f"[EXTERNAL] Reflexx Weekly Summary ({start_day} – {end_day})"
            send_postmark_email(manager_email, subject, body)

        conn.commit()

    finally:
        conn.close()

if __name__ == "__main__":
    main()
