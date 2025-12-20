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

# Thresholds (locked)
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
    now_pt = datetime.now(PACIFIC)
    return (now_pt - timedelta(days=1)).date()


def l7_range_ending_yesterday():
    end_day = pacific_yesterday_date()
    start_day = end_day - timedelta(days=6)
    return start_day, end_day


def prior_l7_range(start_day):
    prior_end = start_day - timedelta(days=1)
    prior_start = prior_end - timedelta(days=6)
    return prior_start, prior_end


def fmt_delta(x):
    # x is change in Z (this week - prior week)
    arrow = "▲" if x > 0 else ("▼" if x < 0 else "•")
    return f"{arrow} {abs(x):.2f}σ"


def bucket_label(key):
    return {"phone": "Phone", "quote": "Quoting", "movement": "Movement"}.get(key, key)


def composite_z(phone_z, quote_z, move_z):
    # Weight phone + quoting most; movement supportive
    return (0.4 * phone_z) + (0.4 * quote_z) + (0.2 * move_z)


# -----------------------------
# DB pulls
# -----------------------------
def get_enabled_managers(conn):
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT id, email, COALESCE(nickname, email) AS name
        FROM users
        WHERE role='manager'
          AND manager_summary_weekly_enabled=1
          AND email IS NOT NULL
          AND email <> ''
        """
    )
    rows = cur.fetchall() or []
    cur.close()
    return rows


def pull_office_z_avg(conn, manager_id, start_day, end_day):
    """
    Office-level averages of L7 Z-scores for the window.
    (Averages across all rows in fact_daily for users under this manager.)
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          AVG(fd.phone_ce_l7_z)    AS phone_z,
          AVG(fd.quote_ce_l7_z)    AS quote_z,
          AVG(fd.movement_ce_l7_z) AS movement_z,
          COUNT(*)                 AS rows_ct
        FROM fact_daily fd
        JOIN users u ON u.id = fd.user_id
        WHERE u.manager_id = %s
          AND u.role = 'user'
          AND fd.date >= %s
          AND fd.date <= %s
          AND (fd.phone_ce_l7_z IS NOT NULL OR fd.quote_ce_l7_z IS NOT NULL OR fd.movement_ce_l7_z IS NOT NULL)
        """,
        (manager_id, str(start_day), str(end_day)),
    )
    row = cur.fetchone() or {}
    cur.close()
    return {
        "phone_z": safe_float(row.get("phone_z"), 0.0),
        "quote_z": safe_float(row.get("quote_z"), 0.0),
        "movement_z": safe_float(row.get("movement_z"), 0.0),
        "rows_ct": safe_int(row.get("rows_ct"), 0),
    }


def pull_reps_week(conn, manager_id, start_day, end_day):
    """
    Per-rep:
      - L7 Z averages for phone/quote/movement
      - plus raw totals (used ONLY for deep-dive within the focus bucket)
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          u.id AS user_id,
          COALESCE(u.nickname, u.email) AS name,

          -- Z-score signals (primary)
          AVG(fd.phone_ce_l7_z)    AS phone_z,
          AVG(fd.quote_ce_l7_z)    AS quote_z,
          AVG(fd.movement_ce_l7_z) AS movement_z,

          -- Raw metrics for bucket deep dive (secondary)
          COALESCE(SUM(fd.inbounds),0)        AS inbounds,
          COALESCE(SUM(fd.outbounds),0)       AS outbounds,
          COALESCE(SUM(fd.ib_time_minutes),0) AS ib_mins,
          COALESCE(SUM(fd.ob_time_minutes),0) AS ob_mins,

          COALESCE(SUM(fd.quoted_items),0)    AS quoted_items,
          COALESCE(SUM(fd.quotes_unique),0)   AS quotes_unique,

          COALESCE(SUM(fd.idle_time_seconds),0)   AS idle_seconds,
          COALESCE(SUM(fd.advisor_pro_minutes),0) AS advisor_pro_minutes,

          COUNT(fd.date) AS days_with_rows
        FROM users u
        LEFT JOIN fact_daily fd
          ON fd.user_id = u.id
         AND fd.date >= %s
         AND fd.date <= %s
        WHERE u.role='user'
          AND u.manager_id=%s
        GROUP BY u.id, u.nickname, u.email
        ORDER BY name ASC
        """,
        (str(start_day), str(end_day), manager_id),
    )
    rows = cur.fetchall() or []
    cur.close()

    # Normalize numeric fields
    for r in rows:
        r["phone_z"] = safe_float(r.get("phone_z"), 0.0)
        r["quote_z"] = safe_float(r.get("quote_z"), 0.0)
        r["movement_z"] = safe_float(r.get("movement_z"), 0.0)
    return rows


# -----------------------------
# Decision logic
# -----------------------------
def strengths_for_rep(rep):
    b = [
        ("phone", rep["phone_z"]),
        ("quote", rep["quote_z"]),
        ("movement", rep["movement_z"]),
    ]
    good = [(k, v) for (k, v) in b if v >= GOOD_Z]
    good.sort(key=lambda x: x[1], reverse=True)
    return good[:2]  # max 2 strengths


def focus_for_rep(rep):
    b = [
        ("phone", rep["phone_z"]),
        ("quote", rep["quote_z"]),
        ("movement", rep["movement_z"]),
    ]
    b.sort(key=lambda x: x[1])  # worst first
    worst_k, worst_v = b[0]
    if worst_v <= ATTN_Z:
        return worst_k, worst_v
    return None, None


def rep_needs_attention(rep):
    return (
        rep["phone_z"] <= ATTN_Z
        or rep["quote_z"] <= ATTN_Z
        or rep["movement_z"] <= ATTN_Z
    )


def top_and_bottom_lists(reps):
    # Composite ranking
    ranked = sorted(
        reps,
        key=lambda r: composite_z(r["phone_z"], r["quote_z"], r["movement_z"]),
        reverse=True,
    )
    top3 = [r["name"] for r in ranked[:3] if r.get("name")]

    # Needs attention = worst bucket first (only those that actually need attention)
    needs = [r for r in reps if rep_needs_attention(r)]
    needs_sorted = sorted(
        needs,
        key=lambda r: min(r["phone_z"], r["quote_z"], r["movement_z"]),
    )
    bottom3 = [r["name"] for r in needs_sorted[:3] if r.get("name")]

    return top3, bottom3


# -----------------------------
# Content builders
# -----------------------------
def build_office_summary_wow(manager_name, this_z, prior_z, start_day, end_day, reps):
    """
    Office summary is ONLY week-over-week variation.
    """
    d_phone = this_z["phone_z"] - prior_z["phone_z"]
    d_quote = this_z["quote_z"] - prior_z["quote_z"]
    d_move = this_z["movement_z"] - prior_z["movement_z"]

    top3, bottom3 = top_and_bottom_lists(reps)

    attention_ct = sum(1 for r in reps if rep_needs_attention(r))
    strong_ct = sum(1 for r in reps if len(strengths_for_rep(r)) > 0)

    lines = []
    lines.append(f"<h1 style='margin:0 0 6px 0; font-size:22px; font-weight:900;'>Weekly Coaching Summary</h1>")
    lines.append(f"<div style='color:#9fb8d6; font-size:13px; margin-bottom:14px;'>Week: {start_day} → {end_day} (vs prior week)</div>")

    lines.append("<h2 style='margin:14px 0 6px 0; font-size:16px; font-weight:900; color:#4aa3ff;'>Office Summary (Week-over-Week)</h2>")
    lines.append("<div style='font-size:14px; line-height:1.45;'>")
    lines.append(f"• <b>Phone</b>: {fmt_delta(d_phone)}")
    lines.append(f"<br>• <b>Quoting</b>: {fmt_delta(d_quote)}")
    lines.append(f"<br>• <b>Movement</b>: {fmt_delta(d_move)}")
    lines.append("</div>")

    lines.append("<div style='margin-top:10px; font-size:14px; line-height:1.45;'>")
    lines.append(f"• <b>Team signals</b>: {strong_ct} rep(s) with a clear strength (≥ +0.5σ), {attention_ct} rep(s) needing attention (≤ -0.5σ).")
    lines.append("</div>")

    if top3:
        lines.append(f"<div style='margin-top:10px; font-size:14px;'><b>Top performers</b>: {', '.join(top3)}</div>")
    if bottom3:
        lines.append(f"<div style='margin-top:6px; font-size:14px;'><b>Needs attention</b>: {', '.join(bottom3)}</div>")

    lines.append("<hr style='border:none; border-top:1px solid rgba(255,255,255,0.12); margin:16px 0;'>")
    return "\n".join(lines)


def build_rep_block(rep):
    name = rep.get("name", "Rep")

    strengths = strengths_for_rep(rep)
    focus_k, focus_v = focus_for_rep(rep)

    # Raw metrics for deep dive (only when needed)
    inb = safe_int(rep.get("inbounds"))
    outb = safe_int(rep.get("outbounds"))
    ib_m = safe_int(rep.get("ib_mins"))
    ob_m = safe_int(rep.get("ob_mins"))
    talk_total = ib_m + ob_m

    quoted_items = safe_int(rep.get("quoted_items"))
    quotes_unique = safe_int(rep.get("quotes_unique"))

    idle_min = minutes_from_seconds(rep.get("idle_seconds"))
    adv_min = safe_int(rep.get("advisor_pro_minutes"))

    # Header
    html = []
    html.append(f"<div style='margin:0 0 12px 0;'>")
    html.append(f"<div style='font-size:16px; font-weight:900; margin:0 0 6px 0;'>{name}</div>")

    # Z snapshot
    html.append(
        f"<div style='font-size:13px; color:#9fb8d6; margin-bottom:8px;'>"
        f"Z (L7): Phone <b>{rep['phone_z']:.2f}σ</b> • Quoting <b>{rep['quote_z']:.2f}σ</b> • Movement <b>{rep['movement_z']:.2f}σ</b>"
        f"</div>"
    )

    # Strengths
    if strengths:
        s_txt = ", ".join([f"{bucket_label(k)} ({v:.2f}σ)" for k, v in strengths])
        html.append(f"<div style='font-size:14px;'><b>Strength</b>: {s_txt}</div>")
    else:
        html.append(f"<div style='font-size:14px; color:#9fb8d6;'><b>Strength</b>: (none ≥ +0.5σ)</div>")

    # Focus + deep dive actions
    if focus_k:
        html.append(f"<div style='font-size:14px; margin-top:6px;'><b>Focus</b>: <span style='color:#ff6b6b; font-weight:900;'>{bucket_label(focus_k)} ({focus_v:.2f}σ)</span></div>")

        # Dive into the focus bucket with real drivers
        if focus_k == "phone":
            html.append("<div style='font-size:14px; margin-top:6px; line-height:1.45;'>"
                        f"• Last week phone drivers: Talk <b>{talk_total}</b> mins (IB {ib_m} / OB {ob_m}), "
                        f"Inbounds <b>{inb}</b>, Outbounds <b>{outb}</b>."
                        "</div>")
            html.append("<div style='font-size:14px; margin-top:6px;'><b>Action</b>: Block two protected call sessions daily; frontload outbound calls; minimize short breaks between calls.</div>")

        elif focus_k == "quote":
            html.append("<div style='font-size:14px; margin-top:6px; line-height:1.45;'>"
                        f"• Last week quoting drivers: Quoted Items <b>{quoted_items}</b>, Unique Quotes <b>{quotes_unique}</b>."
                        "</div>")
            html.append("<div style='font-size:14px; margin-top:6px;'><b>Action</b>: Set a daily unique-quote floor; protect a quoting block; reduce restarts/tab-switching to increase completion speed.</div>")

        elif focus_k == "movement":
            html.append("<div style='font-size:14px; margin-top:6px; line-height:1.45;'>"
                        f"• Workflow signals: Idle <b>{idle_min}</b> mins, AdvisorPro <b>{adv_min}</b> mins."
                        "</div>")
            html.append("<div style='font-size:14px; margin-top:6px;'><b>Action</b>: Treat this as workflow friction (not effort). Identify the top 1–2 tools causing stalls; simplify the path; reduce tab switching; run 60–90 minute single-lane work blocks.</div>")

    else:
        html.append(f"<div style='font-size:14px; margin-top:6px; color:#9fb8d6;'><b>Focus</b>: (no bucket ≤ -0.5σ)</div>")

    html.append("</div>")
    html.append("<hr style='border:none; border-top:1px solid rgba(255,255,255,0.10); margin:12px 0;'>")
    return "\n".join(html)


def build_email_html(manager_name, office_html, reps_html):
    return f"""
    <div style="font-family: Arial, Helvetica, sans-serif; background:#0b1220; color:#e9f6ff; padding:18px;">
      <div style="max-width:760px; margin:0 auto; background:#0e151d; border:1px solid rgba(255,255,255,0.08); border-radius:14px; padding:18px;">
        <div style="font-size:14px; color:#9fb8d6; margin-bottom:8px;">Hi {manager_name},</div>
        {office_html}
        <h2 style="margin:0 0 10px 0; font-size:16px; font-weight:900; color:#4aa3ff;">Individual Coaching (This Week)</h2>
        {reps_html}
        <div style="margin-top:14px; font-weight:900;">Happy Selling! 💪</div>
      </div>
    </div>
    """


def build_email_text(manager_name, start_day, end_day, d_phone, d_quote, d_move, top3, bottom3, reps):
    lines = []
    lines.append("WEEKLY COACHING SUMMARY")
    lines.append(f"Week: {start_day} -> {end_day} (vs prior week)")
    lines.append("")
    lines.append("OFFICE SUMMARY (WEEK-OVER-WEEK)")
    lines.append(f"- Phone: {fmt_delta(d_phone)}")
    lines.append(f"- Quoting: {fmt_delta(d_quote)}")
    lines.append(f"- Movement: {fmt_delta(d_move)}")
    if top3:
        lines.append(f"- Top performers: {', '.join(top3)}")
    if bottom3:
        lines.append(f"- Needs attention: {', '.join(bottom3)}")
    lines.append("")
    lines.append("INDIVIDUAL COACHING")
    for r in reps:
        strengths = strengths_for_rep(r)
        focus_k, focus_v = focus_for_rep(r)

        lines.append(f"{r.get('name','Rep')}")
        lines.append(f"  Z(L7): Phone {r['phone_z']:.2f} | Quoting {r['quote_z']:.2f} | Movement {r['movement_z']:.2f}")

        if strengths:
            s_txt = ", ".join([f"{bucket_label(k)} ({v:.2f})" for k, v in strengths])
            lines.append(f"  Strength: {s_txt}")
        else:
            lines.append("  Strength: (none >= +0.5)")

        if focus_k:
            lines.append(f"  Focus: {bucket_label(focus_k)} ({focus_v:.2f})")
        else:
            lines.append("  Focus: (no bucket <= -0.5)")
        lines.append("")
    lines.append("Happy Selling!")
    return "\n".join(lines)


# -----------------------------
# Postmark
# -----------------------------
def send_postmark_email(to_email, subject, text_body, html_body):
    if not POSTMARK_API_TOKEN:
        raise RuntimeError("Missing POSTMARK_API_TOKEN")

    payload = {
        "From": f"ReflexxApp <{FROM_EMAIL}>",
        "To": to_email,
        "Subject": subject,
        "TextBody": text_body,
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
        managers = get_enabled_managers(conn)

        for m in managers:
            manager_id = m["id"]
            manager_email = m["email"]
            manager_name = m.get("name") or "Manager"

            # Office Z avgs (this week vs prior week)
            this_z = pull_office_z_avg(conn, manager_id, start_day, end_day)
            prior_z = pull_office_z_avg(conn, manager_id, prior_start, prior_end)

            d_phone = this_z["phone_z"] - prior_z["phone_z"]
            d_quote = this_z["quote_z"] - prior_z["quote_z"]
            d_move = this_z["movement_z"] - prior_z["movement_z"]

            reps = pull_reps_week(conn, manager_id, start_day, end_day)

            top3, bottom3 = top_and_bottom_lists(reps)

            # HTML sections
            office_html = build_office_summary_wow(manager_name, this_z, prior_z, start_day, end_day, reps)
            reps_html = "\n".join([build_rep_block(r) for r in reps])

            html_body = build_email_html(manager_name, office_html, reps_html)
            text_body = build_email_text(manager_name, start_day, end_day, d_phone, d_quote, d_move, top3, bottom3, reps)

            subject = f"[EXTERNAL] Reflexx Weekly Coaching Summary ({start_day} – {end_day})"
            send_postmark_email(manager_email, subject, text_body, html_body)

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
