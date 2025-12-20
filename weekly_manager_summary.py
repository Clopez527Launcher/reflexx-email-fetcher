import os
import json
import math
import requests
import mysql.connector
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Los_Angeles")

POSTMARK_API_TOKEN = os.getenv("POSTMARK_API_TOKEN", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", "no-reply@reflexxapp.com")

MYSQLHOST = os.getenv("MYSQLHOST")
MYSQLUSER = os.getenv("MYSQLUSER")
MYSQLPASSWORD = os.getenv("MYSQLPASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # optional (polish language)

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQLHOST,
        user=MYSQLUSER,
        password=MYSQLPASSWORD,
        database=MYSQL_DATABASE,
        autocommit=False,
    )

def pacific_yesterday_date():
    now_pt = datetime.now(PACIFIC)
    return (now_pt - timedelta(days=1)).date()

def l7_range_ending_yesterday():
    end_day = pacific_yesterday_date()
    start_day = end_day - timedelta(days=6)
    return start_day, end_day

def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except:
        return default

def safe_int(x, default=0):
    try:
        if x is None:
            return default
        return int(x)
    except:
        return default

def minutes_from_seconds(sec):
    return round(safe_float(sec, 0.0) / 60.0, 0)

# -----------------------------
# DB pulls
# -----------------------------
def get_enabled_managers(conn):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, email, COALESCE(nickname, email) AS name
        FROM users
        WHERE role='manager'
          AND manager_summary_weekly_enabled=1
          AND email IS NOT NULL
          AND email <> ''
    """)
    rows = cur.fetchall() or []
    cur.close()
    return rows

def get_team_users(conn, manager_id):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, email, COALESCE(nickname, email) AS name
        FROM users
        WHERE role='user'
          AND manager_id=%s
        ORDER BY name ASC
    """, (manager_id,))
    rows = cur.fetchall() or []
    cur.close()
    return rows

def pull_team_aggregate_l7(conn, manager_id, start_day, end_day):
    """Office-level totals/averages over fact_daily for L-7."""
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          COUNT(DISTINCT fd.user_id) AS reps_with_data,
          COUNT(*) AS rows_days,

          COALESCE(SUM(fd.inbounds),0) AS inbounds,
          COALESCE(SUM(fd.outbounds),0) AS outbounds,
          COALESCE(SUM(fd.ib_time_minutes),0) AS ib_mins,
          COALESCE(SUM(fd.ob_time_minutes),0) AS ob_mins,

          COALESCE(SUM(fd.quoted_items),0) AS quoted_items,
          COALESCE(SUM(fd.quotes_unique),0) AS quotes_unique,

          COALESCE(SUM(fd.vc_policies),0) AS vc_policies,
          COALESCE(SUM(fd.vc_items),0) AS vc_items,
          COALESCE(SUM(fd.vc_premium),0) AS vc_premium,

          COALESCE(SUM(fd.idle_time_seconds),0) AS idle_seconds,
          COALESCE(SUM(fd.advisor_pro_minutes),0) AS advisor_pro_minutes,

          COALESCE(AVG(fd.phone_activity_score),0) AS avg_phone_score,
          COALESCE(AVG(fd.quote_activity_score),0) AS avg_quote_score,
          COALESCE(AVG(fd.movement_activity_score),0) AS avg_movement_score,
          COALESCE(AVG(fd.binary_vc_score),0) AS avg_binary_vc_score

        FROM fact_daily fd
        JOIN users u ON u.id = fd.user_id
        WHERE u.manager_id=%s
          AND fd.date >= %s
          AND fd.date <= %s
    """, (manager_id, str(start_day), str(end_day)))
    row = cur.fetchone() or {}
    cur.close()
    return row

def pull_per_rep_l7(conn, manager_id, start_day, end_day):
    """Per rep rollups & bucket averages for L-7."""
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          u.id AS user_id,
          COALESCE(u.nickname, u.email) AS name,

          COALESCE(SUM(fd.inbounds),0) AS inbounds,
          COALESCE(SUM(fd.outbounds),0) AS outbounds,
          COALESCE(SUM(fd.ib_time_minutes),0) AS ib_mins,
          COALESCE(SUM(fd.ob_time_minutes),0) AS ob_mins,

          COALESCE(SUM(fd.quoted_items),0) AS quoted_items,
          COALESCE(SUM(fd.quotes_unique),0) AS quotes_unique,

          COALESCE(SUM(fd.vc_policies),0) AS vc_policies,
          COALESCE(SUM(fd.vc_items),0) AS vc_items,
          COALESCE(SUM(fd.vc_premium),0) AS vc_premium,

          COALESCE(SUM(fd.idle_time_seconds),0) AS idle_seconds,
          COALESCE(SUM(fd.advisor_pro_minutes),0) AS advisor_pro_minutes,

          COALESCE(AVG(fd.phone_activity_score),0) AS phone_score,
          COALESCE(AVG(fd.quote_activity_score),0) AS quote_score,
          COALESCE(AVG(fd.movement_activity_score),0) AS movement_score,
          COALESCE(AVG(fd.binary_vc_score),0) AS binary_vc_score,

          COUNT(*) AS days_with_rows

        FROM users u
        LEFT JOIN fact_daily fd
          ON fd.user_id = u.id
         AND fd.date >= %s
         AND fd.date <= %s
        WHERE u.role='user'
          AND u.manager_id=%s
        GROUP BY u.id, u.nickname, u.email
        ORDER BY name ASC
    """, (str(start_day), str(end_day), manager_id))
    rows = cur.fetchall() or []
    cur.close()
    return rows

# -----------------------------
# Coaching logic (human-style)
# -----------------------------
def bucket_rank(rep):
    # Higher is better. Weight phone + quote most; movement is supportive.
    return (
        safe_float(rep.get("phone_score")) * 0.45 +
        safe_float(rep.get("quote_score")) * 0.45 +
        safe_float(rep.get("movement_score")) * 0.10
    )

def pick_strengths_weaknesses(rep):
    buckets = [
        ("Phone", safe_float(rep.get("phone_score"))),
        ("Quoting", safe_float(rep.get("quote_score"))),
        ("Movement", safe_float(rep.get("movement_score"))),
        ("VC", safe_float(rep.get("binary_vc_score"))),
    ]
    buckets_sorted = sorted(buckets, key=lambda x: x[1], reverse=True)
    strength = buckets_sorted[0]
    weakness = buckets_sorted[-1]
    return strength, weakness, buckets

def coaching_for_rep(rep):
    name = rep.get("name", "Rep")

    inb = safe_int(rep.get("inbounds"))
    outb = safe_int(rep.get("outbounds"))
    ib_m = safe_int(rep.get("ib_mins"))
    ob_m = safe_int(rep.get("ob_mins"))
    talk_total = ib_m + ob_m

    quoted_items = safe_int(rep.get("quoted_items"))
    quotes_unique = safe_int(rep.get("quotes_unique"))

    idle_min = minutes_from_seconds(rep.get("idle_seconds"))
    adv_min = safe_int(rep.get("advisor_pro_minutes"))

    phone_score = safe_float(rep.get("phone_score"))
    quote_score = safe_float(rep.get("quote_score"))
    move_score = safe_float(rep.get("movement_score"))
    vc_score = safe_float(rep.get("binary_vc_score"))

    strength, weakness, buckets = pick_strengths_weaknesses(rep)

    # Build coaching: praise + focus on the weakest bucket
    lines = []
    lines.append(f"{name}")
    lines.append(f"- Last week snapshot: Talk {talk_total} mins (IB {ib_m} / OB {ob_m}), Inbounds {inb}, Outbounds {outb}, Quoted Items {quoted_items}, Unique Quotes {quotes_unique}, Idle {int(idle_min)} mins.")

    lines.append(f"- Strength: {strength[0]} (avg score {strength[1]:.2f}).")

    # Weakness deep dive
    w = weakness[0]
    lines.append(f"- Focus this week: {w} (avg score {weakness[1]:.2f}).")

    if w == "Phone":
        if talk_total < 120:
            lines.append("- Coaching: increase talk time with two protected call blocks per day. Keep the first 90 minutes outbound-heavy.")
        if outb < 40:
            lines.append("- Coaching: outbound count is light — set a daily outbound floor and track it at lunch + end of day.")
        lines.append("- Coaching: reduce short gaps between calls — stay in ‘call mode’ during your blocks and batch admin after.")

    elif w == "Quoting":
        if quotes_unique < 8:
            lines.append("- Coaching: quoting volume is low — set a minimum daily unique quote goal and protect a quoting block.")
        if quoted_items < 15:
            lines.append("- Coaching: increase quoted items by tightening your workflow (templates, fewer tab switches, fewer restarts).")
        lines.append("- Coaching: focus on speed-to-quote → quote while the client is engaged, then follow up same day.")

    elif w == "Movement":
        # Movement is “weird” as you said: it often signals navigation/workflow issues.
        if idle_min > 120:
            lines.append("- Coaching: idle is high — likely workflow/navigation friction. Identify the top 1–2 tools causing stalls and simplify the path.")
        if adv_min > 0 and adv_min < 60:
            lines.append("- Coaching: AdvisorPro time is low — either work is happening elsewhere or there’s a navigation bottleneck. Confirm where time is being spent.")
        lines.append("- Coaching: reduce context switching (too many tabs) — pick one workflow lane for 60–90 minutes at a time.")

    else:  # VC weakness
        lines.append("- Coaching: VC contribution is light — set a weekly VC target and review your pipeline daily.")
        lines.append("- Coaching: identify 3 lead sources most likely to convert to VC and prioritize those touches first.")

    lines.append("")  # spacer
    return "\n".join(lines)

def build_office_summary(manager_name, start_day, end_day, team, reps):
    reps_with_data = safe_int(team.get("reps_with_data"))
    inb = safe_int(team.get("inbounds"))
    outb = safe_int(team.get("outbounds"))
    ib_m = safe_int(team.get("ib_mins"))
    ob_m = safe_int(team.get("ob_mins"))
    talk_total = ib_m + ob_m

    quoted_items = safe_int(team.get("quoted_items"))
    quotes_unique = safe_int(team.get("quotes_unique"))

    idle_min = minutes_from_seconds(team.get("idle_seconds"))
    adv_min = safe_int(team.get("advisor_pro_minutes"))

    avg_phone = safe_float(team.get("avg_phone_score"))
    avg_quote = safe_float(team.get("avg_quote_score"))
    avg_move = safe_float(team.get("avg_movement_score"))
    avg_vc = safe_float(team.get("avg_binary_vc_score"))

    # Rank reps by composite
    reps_sorted = sorted(reps, key=bucket_rank, reverse=True)
    top3 = [r.get("name") for r in reps_sorted[:3] if r.get("name")]
    bottom3 = [r.get("name") for r in reps_sorted[-3:] if r.get("name")]

    lines = []
    lines.append("Weekly Manager Summary")
    lines.append("")
    lines.append(f"{manager_name}, here’s what happened over the last 7 days (short-term coaching view).")
    lines.append(f"Week: {start_day} to {end_day}")
    lines.append("")

    lines.append("Office Summary (last week)")
    lines.append(f"- Team activity: Talk {talk_total} mins (IB {ib_m} / OB {ob_m}), Inbounds {inb}, Outbounds {outb}.")
    lines.append(f"- Quoting: Quoted Items {quoted_items}, Unique Quotes {quotes_unique}.")
    lines.append(f"- Workflow signals: Idle {int(idle_min)} mins, AdvisorPro {adv_min} mins.")
    lines.append(f"- Bucket averages: Phone {avg_phone:.2f}, Quoting {avg_quote:.2f}, Movement {avg_move:.2f}, VC {avg_vc:.2f}.")
    if top3:
        lines.append(f"- Top performers (overall): {', '.join(top3)}.")
    if bottom3:
        lines.append(f"- Needs attention (overall): {', '.join(bottom3)}.")
    lines.append("")
    lines.append("Coaching Suggestions (this week)")
    lines.append("- Keep coaching tight: focus each rep on their weakest bucket first; don’t over-coach everything at once.")
    lines.append("")
    return "\n".join(lines)

def ai_polish(text):
    # optional — if you don’t set OPENAI_API_KEY, the email still sends with rule-based coaching
    if not OPENAI_API_KEY:
        return text
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4.1-mini",
                "messages": [
                    {"role": "system",
                     "content": "Rewrite as a weekly manager email. Keep it short, punchy, and actionable. No tables. Preserve names and numbers."},
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

def send_postmark_email(to_email, subject, text_body):
    if not POSTMARK_API_TOKEN:
        raise RuntimeError("Missing POSTMARK_API_TOKEN")

    payload = {
        "From": f"ReflexxApp <{FROM_EMAIL}>",
        "To": to_email,
        "Subject": subject,
        "TextBody": text_body,
        "MessageStream": "outbound",
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

def main():
    start_day, end_day = l7_range_ending_yesterday()

    conn = get_db_connection()
    try:
        managers = get_enabled_managers(conn)

        for m in managers:
            manager_id = m["id"]
            manager_email = m["email"]
            manager_name = m.get("name") or "Manager"

            team = pull_team_aggregate_l7(conn, manager_id, start_day, end_day)
            reps = pull_per_rep_l7(conn, manager_id, start_day, end_day)

            # Build email
            parts = []
            parts.append(build_office_summary(manager_name, start_day, end_day, team, reps))

            # Per rep coaching (no tables, short)
            for rep in reps:
                parts.append(coaching_for_rep(rep))

            parts.append("Happy Selling!")

            body = "\n".join(parts)
            body = ai_polish(body)

            subject = f"[EXTERNAL] Reflexx Weekly Coaching Summary ({start_day} – {end_day})"
            send_postmark_email(manager_email, subject, body)

        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
