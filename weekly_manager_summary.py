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
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0):
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def minutes_from_seconds(sec):
    return int(round(safe_float(sec, 0.0) / 60.0, 0))


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


def gl_status(a, b):
    """
    Week over week status label with color.
    - Up = green
    - Average = yellow
    - Down = red
    """
    if a > b + 0.05:
        return "<span style='color:#31d07f; font-weight:900;'>🟢 Up</span>"
    if a < b - 0.05:
        return "<span style='color:#ff5b5b; font-weight:900;'>🔴 Down</span>"
    return "<span style='color:#f5d04c; font-weight:900;'>🟡 Average</span>"


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
        "phone_z": safe_float(row.get("phone_z"), 0.0),
        "quote_z": safe_float(row.get("quote_z"), 0.0),
        "movement_z": safe_float(row.get("movement_z"), 0.0),
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
          COALESCE(SUM(fd.inbounds),0)         AS inbounds,
          COALESCE(SUM(fd.outbounds),0)        AS outbounds,
          COALESCE(SUM(fd.ib_time_minutes),0)  AS ib_mins,
          COALESCE(SUM(fd.ob_time_minutes),0)  AS ob_mins,
          COALESCE(SUM(fd.quoted_items),0)     AS quoted_items,
          COALESCE(SUM(fd.quotes_unique),0)    AS quotes_unique,
          COALESCE(SUM(fd.idle_time_seconds),0) AS idle_seconds,
          COALESCE(SUM(fd.advisor_pro_minutes),0) AS advisor_pro_minutes
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
    strengths_list = []
    if rep["phone_z"] >= GOOD_Z:
        strengths_list.append("phone")
    if rep["quote_z"] >= GOOD_Z:
        strengths_list.append("quote")
    if rep["movement_z"] >= GOOD_Z:
        strengths_list.append("movement")
    return strengths_list


def composite(rep):
    return (0.4 * rep["phone_z"]) + (0.4 * rep["quote_z"]) + (0.2 * rep["movement_z"])


# -----------------------------
# Coaching copy - (2–3 sentences with meat)
# -----------------------------
def coaching_sentences(rep, focus):
    """
    Returns 2–3 sentences of manager-readable coaching.
    Uses existing raw totals for specificity (no decimals).
    Adds occasional credibility line (not for every rep).
    """
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

    # Light credibility examples (only sometimes)
    credibility_quote = (
        "<span style='color:#9fb8d6;'>Quick note: Teams that add a protected 60–90 minute quoting block "
        "and batch follow-ups (instead of bouncing tabs) typically see faster quote completion within 1–2 weeks.</span>"
    )
    credibility_phone = (
        "<span style='color:#9fb8d6;'>Quick note: Top producers usually run outbound in time blocks "
        "(ex: 2 blocks/day) so the day doesn’t get eaten by little interruptions.</span>"
    )

    if focus == "quote":
        # Use numbers only as plain counts (no stats)
        s1 = f"Last week they produced <b>{quotes_unique}</b> unique quotes (<b>{quoted_items}</b> quoted items)."
        s2 = "Coach this by protecting <b>one uninterrupted 60–90 minute quoting block</b> daily (no email, no resets, no tab-hopping)."
        s3 = "During that block: finish the quote to submission, then batch follow-ups after the block so momentum stays high."
        # Add credibility for SOME reps (example: only if quoting focus and activity is non-zero)
        extra = f"<div style='margin-top:6px;'>{credibility_quote}</div>" if quotes_unique > 0 else ""
        return f"<div style='margin-top:6px; line-height:1.45;'>{s1}<br>{s2}<br>{s3}</div>{extra}"

    if focus == "phone":
        s1 = f"Last week phone activity was <b>{talk_total}</b> talk minutes (IB {ib_m} / OB {ob_m}) with <b>{outb}</b> outbound and <b>{inb}</b> inbound calls."
        s2 = "Coach this by scheduling <b>two protected call blocks</b> per day (ex: 9–10:30 and 2–3) and front-loading outbound in the first block."
        s3 = "Goal is fewer stop/starts: keep the dialer open, run through a list, and batch admin work after the block."
        extra = f"<div style='margin-top:6px;'>{credibility_phone}</div>" if outb > 0 else ""
        return f"<div style='margin-top:6px; line-height:1.45;'>{s1}<br>{s2}<br>{s3}</div>{extra}"

    if focus == "movement":
        s1 = f"Workflow signals showed <b>{idle_min}</b> idle minutes and <b>{adv_min}</b> minutes in AdvisorPro."
        s2 = "Coach this as <b>workflow friction</b>, not effort: identify the top 1–2 tools/pages causing stalls and simplify the path (templates, saved steps, fewer tabs)."
        s3 = "Run <b>single-lane work blocks</b> (60 minutes) where they finish one task type end-to-end before switching."
        return f"<div style='margin-top:6px; line-height:1.45;'>{s1}<br>{s2}<br>{s3}</div>"

    # No focus
    return "<div style='margin-top:6px; color:#9fb8d6; line-height:1.45;'>No major coaching flags this week. Keep momentum and reinforce what’s working.</div>"


# -----------------------------
# Content builders
# -----------------------------
def build_office_summary(start_day, end_day, this_z, prior_z, reps):
    top = sorted(reps, key=composite, reverse=True)[:3]
    needs = [r for r in reps if primary_focus(r)]

    phone_status = gl_status(this_z["phone_z"], prior_z["phone_z"])
    quote_status = gl_status(this_z["quote_z"], prior_z["quote_z"])
    move_status = gl_status(this_z["movement_z"], prior_z["movement_z"])

    return f"""
    <h1 style="margin:0;font-size:22px;font-weight:900;">Weekly Coaching Summary</h1>
    <div style="color:#9fb8d6;margin-bottom:14px;">Week: {start_day} → {end_day}</div>

    <h2 style="font-size:16px;color:#4aa3ff;font-weight:900;">This Week at a Glance</h2>
    <ul style="line-height:1.7; margin-top:8px;">
      <li><b>Phone:</b> {phone_status}</li>
      <li><b>Quoting:</b> {quote_status}</li>
      <li><b>Workflow:</b> {move_status}</li>
    </ul>

    <div style="margin-top:10px; line-height:1.6;">
      <b>Top performers:</b> {", ".join(r["name"] for r in top) if top else "—"}<br>
      <b>Needs attention:</b> {", ".join(r["name"] for r in needs[:3]) if needs else "—"}
    </div>

    <hr style="margin:16px 0; border:none; border-top:1px solid rgba(255,255,255,0.10);">
    """


def build_rep_block(rep):
    focus = primary_focus(rep)
    strengths_list = strengths(rep)

    html = f"""
    <div style="margin-bottom:12px;">
      <div style="font-size:16px;font-weight:900;">{rep["name"]}</div>
      <div><b>Strength:</b> {", ".join(bucket_label(s) for s in strengths_list) if strengths_list else "—"}</div>
      <div><b>Primary Coaching Focus:</b> {bucket_label(focus) if focus else "None this week"}</div>
      {coaching_sentences(rep, focus)}
      <hr style="margin:12px 0; border:none; border-top:1px solid rgba(255,255,255,0.10);">
    </div>
    """
    return html


def build_email_html(manager_name, office_html, reps_html):
    return f"""
    <div style="font-family:Arial, Helvetica, sans-serif;background:#0b1220;color:#e9f6ff;padding:18px;">
      <div style="max-width:760px;margin:auto;background:#0e151d;padding:18px;border-radius:14px;border:1px solid rgba(255,255,255,0.08);">
        <div style="color:#9fb8d6;margin-bottom:8px;">Hi {manager_name},</div>
        {office_html}
        <h2 style="font-size:16px;color:#4aa3ff;font-weight:900;margin-top:0;">Individual Coaching</h2>
        {reps_html}
        <div style="margin-top:14px;font-weight:900;">Happy Selling 💪</div>
      </div>
    </div>
    """


# -----------------------------
# Send
# -----------------------------
def send_postmark_email(to_email, subject, html_body):
    if not POSTMARK_API_TOKEN:
        raise RuntimeError("Missing POSTMARK_API_TOKEN")

    payload = {
        "From": f"Reflexx <{FROM_EMAIL}>",
        "To": to_email,
        "Subject": subject,
        "HtmlBody": html_body,
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
