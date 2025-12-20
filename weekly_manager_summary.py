import os
import json
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

# Internal thresholds (HIDDEN from managers)
GOOD_Z = 0.5
ATTN_Z = -0.5


# -----------------------------
# Helpers
# -----------------------------
def get_db_connection():
    return mysql.connector.connect(
        host=MYSQLHOST,
        user=MYSQLUSER,
        password=MYSQLPASSWORD,
        database=MYSQL_DATABASE,
        autocommit=False,
    )


def safe_float(x, default=0.0):
    try:
        return float(x or default)
    except Exception:
        return default


def safe_int(x, default=0):
    try:
        return int(x or default)
    except Exception:
        return default


def minutes_from_seconds(sec):
    return int(round(safe_float(sec) / 60.0, 0))


def pacific_yesterday_date():
    return (datetime.now(PACIFIC) - timedelta(days=1)).date()


def l7_range_ending_yesterday():
    end_day = pacific_yesterday_date()
    start_day = end_day - timedelta(days=6)
    return start_day, end_day


def prior_l7_range(start_day):
    prior_end = start_day - timedelta(days=1)
    prior_start = prior_end - timedelta(days=6)
    return prior_start, prior_end


def bucket_label(k):
    return {"phone": "Phone", "quote": "Quoting", "movement": "Workflow"}.get(k, k)


def bucket_status(z):
    if z >= GOOD_Z:
        return "🟢 Strong"
    if z <= ATTN_Z:
        return "🔴 Needs Attention"
    return "🟡 Watch"


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
          AND email IS NOT NULL AND email <> ''
    """)
    rows = cur.fetchall() or []
    cur.close()
    return rows


def pull_office_z_avg(conn, manager_id, start_day, end_day):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          AVG(fd.phone_ce_l7_z)    AS phone_z,
          AVG(fd.quote_ce_l7_z)    AS quote_z,
          AVG(fd.movement_ce_l7_z) AS movement_z
        FROM fact_daily fd
        JOIN users u ON u.id = fd.user_id
        WHERE u.manager_id=%s
          AND u.role='user'
          AND fd.date BETWEEN %s AND %s
    """, (manager_id, str(start_day), str(end_day)))
    row = cur.fetchone() or {}
    cur.close()
    return {
        "phone_z": safe_float(row.get("phone_z")),
        "quote_z": safe_float(row.get("quote_z")),
        "movement_z": safe_float(row.get("movement_z")),
    }


def pull_reps_week(conn, manager_id, start_day, end_day):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          u.id,
          COALESCE(u.nickname, u.email) AS name,
          AVG(fd.phone_ce_l7_z)    AS phone_z,
          AVG(fd.quote_ce_l7_z)    AS quote_z,
          AVG(fd.movement_ce_l7_z) AS movement_z,
          SUM(fd.inbounds)         AS inbounds,
          SUM(fd.outbounds)        AS outbounds,
          SUM(fd.ib_time_minutes)  AS ib_mins,
          SUM(fd.ob_time_minutes)  AS ob_mins,
          SUM(fd.quoted_items)     AS quoted_items,
          SUM(fd.quotes_unique)    AS quotes_unique,
          SUM(fd.idle_time_seconds) AS idle_seconds,
          SUM(fd.advisor_pro_minutes) AS advisor_pro_minutes
        FROM users u
        LEFT JOIN fact_daily fd
          ON fd.user_id=u.id
         AND fd.date BETWEEN %s AND %s
        WHERE u.manager_id=%s AND u.role='user'
        GROUP BY u.id
        ORDER BY name
    """, (str(start_day), str(end_day), manager_id))
    rows = cur.fetchall() or []
    cur.close()
    
    # ✅ Normalize AVG() Decimal values to float so math works
    for r in rows:
        r["phone_z"] = safe_float(r.get("phone_z"), 0.0)
        r["quote_z"] = safe_float(r.get("quote_z"), 0.0)
        r["movement_z"] = safe_float(r.get("movement_z"), 0.0)

        # Also normalize names just in case
        r["name"] = r.get("name") or "Rep"

    return rows


# -----------------------------
# Decision logic
# -----------------------------
def primary_focus(rep):
    buckets = {
        "phone": rep["phone_z"],
        "quote": rep["quote_z"],
        "movement": rep["movement_z"],
    }
    worst = min(buckets, key=buckets.get)
    if buckets[worst] <= ATTN_Z:
        return worst
    return None


def strengths(rep):
    return [k for k, v in {
        "phone": rep["phone_z"],
        "quote": rep["quote_z"],
        "movement": rep["movement_z"],
    }.items() if v >= GOOD_Z]


def composite(rep):
    return (0.4 * rep["phone_z"]) + (0.4 * rep["quote_z"]) + (0.2 * rep["movement_z"])


# -----------------------------
# Content builders
# -----------------------------
def build_office_summary(start_day, end_day, this_z, prior_z, reps):
    def trend(a, b):
        if a > b + 0.05: return "Improved"
        if a < b - 0.05: return "Down"
        return "Flat"

    top = sorted(reps, key=composite, reverse=True)[:3]
    needs = [r for r in reps if primary_focus(r)]

    return f"""
    <h1 style="margin:0;font-size:22px;">Weekly Coaching Summary</h1>
    <div style="color:#9fb8d6;margin-bottom:14px;">Week: {start_day} → {end_day}</div>

    <h2 style="font-size:16px;color:#4aa3ff;">This Week at a Glance</h2>
    <ul style="line-height:1.6;">
      <li><b>Phone:</b> {trend(this_z["phone_z"], prior_z["phone_z"])}</li>
      <li><b>Quoting:</b> {trend(this_z["quote_z"], prior_z["quote_z"])}</li>
      <li><b>Workflow:</b> {trend(this_z["movement_z"], prior_z["movement_z"])}</li>
    </ul>

    <div style="margin-top:10px;">
      <b>Top performers:</b> {", ".join(r["name"] for r in top) if top else "—"}<br>
      <b>Needs attention:</b> {", ".join(r["name"] for r in needs[:3]) if needs else "—"}
    </div>

    <hr style="margin:16px 0;">
    """


def build_rep_block(rep):
    focus = primary_focus(rep)
    strengths_list = strengths(rep)

    html = f"""
    <div style="margin-bottom:12px;">
      <div style="font-size:16px;font-weight:900;">{rep["name"]}</div>
      <div><b>Strength:</b> {", ".join(bucket_label(s) for s in strengths_list) if strengths_list else "—"}</div>
      <div><b>Primary Coaching Focus:</b> {bucket_label(focus) if focus else "None this week"}</div>
    """

    if focus == "phone":
        html += "<div>Coach on call consistency and protected call blocks.</div>"
    elif focus == "quote":
        html += "<div>Coach on completing quotes with fewer restarts.</div>"
    elif focus == "movement":
        html += "<div>Coach on workflow friction, not effort.</div>"

    html += "<hr style='margin:12px 0;'></div>"
    return html


def build_email_html(manager_name, office_html, reps_html):
    return f"""
    <div style="font-family:Arial;background:#0b1220;color:#e9f6ff;padding:18px;">
      <div style="max-width:760px;margin:auto;background:#0e151d;padding:18px;border-radius:14px;">
        <div style="color:#9fb8d6;">Hi {manager_name},</div>
        {office_html}
        <h2 style="font-size:16px;color:#4aa3ff;">Individual Coaching</h2>
        {reps_html}
        <div style="margin-top:14px;font-weight:900;">Happy Selling 💪</div>
      </div>
    </div>
    """


# -----------------------------
# Send
# -----------------------------
def send_postmark_email(to_email, subject, html_body):
    payload = {
        "From": f"Reflexx <{FROM_EMAIL}>",
        "To": to_email,
        "Subject": subject,
        "HtmlBody": html_body,
        "MessageStream": "outbound",
    }
    requests.post(
        "https://api.postmarkapp.com/email",
        headers={
            "X-Postmark-Server-Token": POSTMARK_API_TOKEN,
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=30,
    )


# -----------------------------
# Main
# -----------------------------
def main():
    start_day, end_day = l7_range_ending_yesterday()
    prior_start, prior_end = prior_l7_range(start_day)

    conn = get_db_connection()
    try:
        for m in get_enabled_managers(conn):
            reps = pull_reps_week(conn, m["id"], start_day, end_day)
            office_now = pull_office_z_avg(conn, m["id"], start_day, end_day)
            office_prev = pull_office_z_avg(conn, m["id"], prior_start, prior_end)

            office_html = build_office_summary(start_day, end_day, office_now, office_prev, reps)
            reps_html = "".join(build_rep_block(r) for r in reps)

            html = build_email_html(m["name"], office_html, reps_html)

            send_postmark_email(
                m["email"],
                f"[EXTERNAL] Reflexx Weekly Coaching Summary ({start_day} – {end_day})",
                html
            )

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
