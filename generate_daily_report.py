import os
import json
import decimal
from io import BytesIO
from datetime import datetime, date, timedelta

import mysql.connector
from pytz import timezone, utc

from reportlab.lib.pagesizes import LETTER
from reportlab.platypus import Table, TableStyle, SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


# ---------- Font setup ----------
AMASIS_REG_TTF  = "AmasisMTPro-Regular.ttf"
AMASIS_BOLD_TTF = "AmasisMTPro-Bold.ttf"

FONT_MAIN = "Helvetica"
FONT_BOLD = "Helvetica-Bold"
try:
    if os.path.exists(AMASIS_REG_TTF):
        pdfmetrics.registerFont(TTFont("AmasisMTPro", AMASIS_REG_TTF))
        FONT_MAIN = "AmasisMTPro"
    if os.path.exists(AMASIS_BOLD_TTF):
        pdfmetrics.registerFont(TTFont("AmasisMTPro-Bold", AMASIS_BOLD_TTF))
        FONT_BOLD = "AmasisMTPro-Bold"
except Exception:
    pass


# ---------- MySQL config ----------
def _env(*names):
    """Return the first env var value that exists and is non-empty."""
    for n in names:
        v = os.getenv(n)
        if v and str(v).strip():
            return v
    return None

MYSQL_CONFIG = {
    "host": _env("MYSQLHOST", "MYSQL_HOST") or "mysql.railway.internal",
    "port": int(_env("MYSQLPORT", "MYSQL_PORT") or 3306),
    "user": _env("MYSQLUSER", "MYSQL_USER") or "root",
    "password": _env(
        "MYSQLPASSWORD",
        "MYSQL_PASSWORD",
        "MYSQL_ROOT_PASSWORD"
    ),
    "database": _env("MYSQLDATABASE", "MYSQL_DATABASE") or "railway",
}


# ---------- Helpers ----------
# -------------------------------
# ✅ FACT_DAILY: pull yesterday (Pacific) for this manager
# -------------------------------
def fetch_fact_daily_for_manager(conn, manager_id, pacific_yesterday_date):
    """
    Returns list[dict] of fact_daily rows for all users under a manager for a single day.
    pacific_yesterday_date is a python date (YYYY-MM-DD)
    """
    cur = conn.cursor(dictionary=True)

    sql = """
        SELECT
            fd.date,
            fd.user_id,
            u.email AS email,
            COALESCE(fd.user_name, u.email) AS user_name,

            fd.outbounds,
            fd.ib_time_minutes,
            fd.ob_time_minutes,
            (COALESCE(fd.ib_time_minutes,0) + COALESCE(fd.ob_time_minutes,0)) AS total_talk_minutes,

            fd.advisor_pro_minutes,

            fd.phone_activity_score,
            fd.movement_activity_score,
            fd.quote_activity_score,
            fd.binary_vc_score,

            fd.idle_time_seconds,
            ROUND((COALESCE(fd.idle_time_seconds,0) / 60.0), 0) AS idle_minutes,

            fd.quoted_items,
            fd.quotes_unique,
            fd.vc_policies,
            fd.vc_items
        FROM fact_daily fd
        JOIN users u ON u.id = fd.user_id
        WHERE fd.date = %s
          AND (
              u.manager_id = %s
              OR u.id = %s
          )
        ORDER BY COALESCE(fd.user_name, u.email)
    """

    cur.execute(sql, (pacific_yesterday_date, manager_id, manager_id))
    return cur.fetchall()

def pacific_day_utc_window(target_local_date: date):
    """Return (start_utc, end_utc, pacific_date_str) for the given Pacific calendar date."""
    pac = timezone("US/Pacific")
    start_local = pac.localize(datetime(target_local_date.year, target_local_date.month, target_local_date.day, 0, 0, 0, 0))
    end_local   = pac.localize(datetime(target_local_date.year, target_local_date.month, target_local_date.day, 23, 59, 59, 999999))
    return start_local.astimezone(utc), end_local.astimezone(utc), target_local_date.strftime("%Y-%m-%d")


def hms_to_secs(s: str) -> int:
    if not s or s == "0:00:00":
        return 0
    try:
        hh, mm, ss = s.split(":")
        return int(hh) * 3600 + int(mm) * 60 + int(ss)
    except Exception:
        return 0


def secs_to_hms(total_secs: int) -> str:
    total_secs = int(total_secs or 0)
    h = total_secs // 3600
    m = (total_secs % 3600) // 60
    s = total_secs % 60
    return f"{h}:{m:02d}:{s:02d}"

def time_to_hms(x) -> str:
    """
    Ensure TIME values are always JSON-safe strings.
    mysql TIME can come back as timedelta.
    """
    if x is None:
        return "0:00:00"
    if isinstance(x, timedelta):
        total = int(x.total_seconds())
        return secs_to_hms(total)
    return str(x)


def safe_int(x):
    try:
        return int(x or 0)
    except Exception:
        return 0

def normalize_ai_language(text: str) -> str:
    if not text:
        return text

    replacements = {
        "increase talk minutes": "increase their talk time",
        "increase their total talk minutes significantly": "increase their talk time",
        "increase total talk minutes": "increase their talk time",
        "increase talk minute": "increase their talk time",
    }

    for bad, good in replacements.items():
        text = text.replace(bad, good)

    return text
    
def fetch_index_scores_for_manager(conn, manager_id: int, target_day: date):
    """
    Returns dict[user_id] = index_score for a specific day.
    Index Score = (daily_elite_calls / daily_talk_seconds) * 60 * 100
    (same idea as "elite per minute" scaled by 100)
    """
    cur = conn.cursor(dictionary=True)

    sql = """
        SELECT
            user_id,
            daily_elite_calls,
            daily_talk_seconds
        FROM elite_calls_fact_daily
        WHERE manager_id = %s
          AND day = %s
    """
    cur.execute(sql, (manager_id, target_day))

    out = {}
    for r in cur.fetchall():
        elite = float(r.get("daily_elite_calls") or 0)
        talk_secs = float(r.get("daily_talk_seconds") or 0)

        if talk_secs <= 0:
            score = 0.0
        else:
            score = (elite / talk_secs) * 60.0 * 100.0

        out[int(r["user_id"])] = round(score, 2)

    return out    

# ---------- AI summaries (office + per rep) ----------
def get_ai_summaries(fact_rows, pacific_date_str: str):
    """
    agent_data rows look like:
      [name, inbound, outbound, in_talk, out_talk]

    Returns:
      office_summary: str (3 sentences)
      rep_summaries: dict[str,str] (2 sentences each)
    """
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        office = "AI summary unavailable (OPENAI_API_KEY not set)."
        reps = {(r.get("user_name") or "Unknown"): office for r in fact_rows}
        return office, reps

    # Ask for STRICT JSON so we can render cleanly (from fact_daily)
    payload = []
    for r in fact_rows:
        payload.append({
            "name": r.get("email") or r.get("user_name"),
            "display_name": r.get("user_name"),
            "outbounds": int(r.get("outbounds") or 0),
            "total_talk_minutes": float(r.get("total_talk_minutes") or 0),
            "advisor_pro_minutes": int(r.get("advisor_pro_minutes") or 0),
            "movement_activity_score": float(r.get("movement_activity_score") or 0),
            "idle_minutes": float(r.get("idle_minutes") or 0),
        })

    prompt = f"""
You are Reflexx AI. Analyze yesterday's performance for the office (Pacific date {pacific_date_str}).

IMPORTANT:
- Idle time: LOW idle_minutes is GOOD. HIGH idle_minutes is BAD, especially > 90 minutes.
- Use ONLY these fields: outbounds, total_talk_minutes, advisor_pro_minutes, movement_activity_score, idle_minutes.
- Do NOT talk about inbound/outbound talk TIME separately (we already gave total_talk_minutes).
- If values are low or 0, say it plainly.

DATA (per rep):
{json.dumps(payload, indent=2)}

Return STRICT JSON only with this exact shape:
{{
  "office_summary": "THREE short sentences about the office overall. Mention who did well and who struggled (based only on the data).",
  "rep_summaries": [
    {{
      "name": "MUST match the input name exactly (email). Example: jcardona5@allstate.com",
      "summary": "TWO short sentences about this rep. 1) what they did well, 2) what to improve next."
    }}
  ]
}}

Rules:
- Do NOT invent numbers or facts not in the data.
- Keep it short, direct, and sales-manager style.
- If a metric is zero/low, say it plainly.
- You MUST return one rep_summaries item for EVERY rep in DATA (same count).
- For each rep_summaries item, the "name" MUST EXACTLY equal that rep's input "name" (email). Do NOT use display_name.
- Idle time: low idle_minutes is GOOD. High idle_minutes is BAD, especially > 90 minutes.


""".strip()

    from openai import OpenAI
    client = OpenAI(api_key=key)
    r = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    raw = (r.choices[0].message.content or "").strip()

    # Robust parse: if model adds extra text, try to extract JSON
    try:
        data = json.loads(raw)
    except Exception:
        # try to extract the first {...} block
        try:
            start = raw.find("{")
            end = raw.rfind("}")
            data = json.loads(raw[start:end+1])
        except Exception:
            office = raw if raw else "AI summary error: empty response."
            reps = {(r.get("user_name") or "Unknown"): "AI summary error: could not parse response." for r in fact_rows}
            return office, reps

    office_summary = (data.get("office_summary") or "").strip() or "No office summary returned."
    rep_summaries_list = data.get("rep_summaries") or []

    # ✅ Build maps so we can recover if the model returns display_name instead of email
    expected_keys = []
    display_to_email = {}
    for p in payload:
        k = (p.get("name") or "").strip()  # email preferred
        dn = (p.get("display_name") or "").strip()
        if k:
            expected_keys.append(k)
        if dn and k:
            display_to_email[dn.lower()] = k

    rep_map = {}
    for item in rep_summaries_list:
        n = (item.get("name") or "").strip()
        s = (item.get("summary") or "").strip()
        if not n:
            continue

        # If AI returned display name, map it back to email
        if n not in expected_keys:
            mapped = display_to_email.get(n.lower())
            if mapped:
                n = mapped

        rep_map[n] = s


    # ensure every rep has *something*
    for r in fact_rows:
        key = (r.get("email") or "").strip() or (r.get("user_name") or "").strip() or "Unknown"
        if key not in rep_map:
            rep_map[key] = "No AI summary returned for this rep."

    return office_summary, rep_map


# ---------- Fetch metrics (TZ-correct + manager-filtered) ----------
def fetch_metrics(manager_id: int, pacific_date: date = None):
    """
    Manager-filtered:
      - activity_log (UTC timestamps) using Pacific-day UTC window
      - call_metrics per user by Pacific DATE (report_date)
    """
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    if pacific_date is None:
        # ✅ default to Pacific yesterday (matches your app mindset)
        pacific_date = (datetime.now(timezone("US/Pacific")).date() - timedelta(days=1))

    start_utc, end_utc, pacific_date_str = pacific_day_utc_window(pacific_date)

    # Office totals from activity_log (UTC window) filtered to manager team
    cur.execute("""
        SELECT
          SUM(a.mouse_distance),
          SUM(a.keystrokes),
          SUM(a.mouse_clicks),
          SUM(a.idle_count)
        FROM activity_log a
        JOIN users u ON a.user_id = u.id
        WHERE a.timestamp BETWEEN %s AND %s
          AND (u.manager_id = %s OR u.id = %s)
    """, (start_utc, end_utc, manager_id, manager_id))
    total_activity = cur.fetchone()

    # Per-user activity (UTC window) filtered to manager team
    cur.execute("""
        SELECT
          u.email,
          SUM(a.mouse_distance),
          SUM(a.keystrokes),
          SUM(a.mouse_clicks),
          SUM(a.idle_count)
        FROM activity_log a
        JOIN users u ON a.user_id = u.id
        WHERE a.timestamp BETWEEN %s AND %s
          AND (u.manager_id = %s OR u.id = %s)
        GROUP BY u.email
        ORDER BY u.email
    """, (start_utc, end_utc, manager_id, manager_id))
    user_activities = cur.fetchall()

    # Per-user call metrics by Pacific date filtered to manager team
    cur.execute("""
        SELECT
          u.email,
          cm.inbound_calls,
          cm.outbound_calls,
          cm.inbound_time,
          cm.outbound_time
        FROM call_metrics cm
        JOIN users u ON cm.user_id = u.id
        WHERE cm.report_date = %s
          AND (u.manager_id = %s OR u.id = %s)
    """, (pacific_date_str, manager_id, manager_id))
    user_calls = {row[0]: row[1:] for row in cur.fetchall()}

    # Web usage JSON (UTC window) filtered to manager team
    cur.execute("""
        SELECT a.page_time
        FROM activity_log a
        JOIN users u ON a.user_id = u.id
        WHERE a.timestamp BETWEEN %s AND %s
          AND (u.manager_id = %s OR u.id = %s)
    """, (start_utc, end_utc, manager_id, manager_id))

    from collections import defaultdict
    def safe_get(v):
        try:
            return float(v) if v is not None else 0.0
        except (decimal.InvalidOperation, ValueError, TypeError):
            return 0.0

    app_totals = defaultdict(float)
    for (blob,) in cur.fetchall():
        if not blob:
            continue
        try:
            data = json.loads(blob)
            for app, secs in data.items():
                app_totals[app] += safe_get(secs)
        except Exception:
            continue

    total_time = sum(app_totals.values()) or 1
    web_usage = sorted(
        [(app, f"{round((t/total_time)*100,2)}") for app, t in app_totals.items()],
        key=lambda x: float(x[1]),
        reverse=True
    )

    cur.close(); conn.close()
    return total_activity, user_activities, user_calls, web_usage, pacific_date_str


# ---------- PDF generation (returns BYTES) ----------
def generate_pdf_bytes(office_summary, rep_summaries, web_usage, pacific_date_str: str,
                       snapshot_yesterday=None, snapshot_l7=None):
    pacific = timezone("US/Pacific")
    now = datetime.now(pacific)
    timestamp = now.strftime('%b %d, %Y at %I:%M %p')
    filename = f"Reflexx Daily Report – {timestamp}.pdf"

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=54,
        rightMargin=54,
        topMargin=54,
        bottomMargin=54
    )
    content_width = doc.width

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name="ReportTitle",
        parent=styles["Title"],
        fontName=FONT_MAIN,
        fontSize=18,
        leading=22
    ))
    styles.add(ParagraphStyle(
        name="Body",
        parent=styles["BodyText"],
        fontName=FONT_MAIN,
        fontSize=10.25,
        leading=14
    ))
    styles.add(ParagraphStyle(
        name="H2",
        parent=styles["Heading2"],
        fontName=FONT_MAIN,
        fontSize=13,
        leading=16
    ))
    styles.add(ParagraphStyle(
        name="HeaderWhiteSmall",
        fontName=FONT_BOLD,
        fontSize=7.0,
        leading=8.6,
        alignment=1,
        textColor=colors.white
    ))

    # ✅ Build elements FIRST (no loops inside the list)
    elements = [
        Paragraph(f"<b>Reflexx Daily Report – {timestamp}</b>", styles["ReportTitle"]),
        Spacer(1, 6),
        Paragraph(f"<i>Data window: Pacific calendar day {pacific_date_str}</i>", styles["Body"]),
        Spacer(1, 10),

        Paragraph("<b>AI Summary – Office</b>", styles["H2"]),
        Spacer(1, 6),
        Paragraph(office_summary or "No office summary returned.", styles["Body"]),
        Spacer(1, 12),

        Paragraph("<b>Bucket Score Snapshot</b>", styles["H2"]),
        Spacer(1, 6),
    ]

    # ✅ helper to build a ReportLab table for the snapshot
    def snapshot_table(title, rows):
        elements.append(Paragraph(f"<b>{title}</b>", styles["Body"]))
        elements.append(Spacer(1, 4))

        if not rows:
            elements.append(Paragraph("No data available.", styles["Body"]))
            elements.append(Spacer(1, 8))
            return

        data = [["Rep", "Phone", "Quote", "Movement", "Index Score"]]
        for r in rows:
            data.append([
                r["name"],
                f'{r["phone"]:.2f}',
                f'{r["quote"]:.2f}',
                f'{r["movement"]:.2f}',
                f'{r["index_score"]:.2f}',
            ])

        t = Table(data, colWidths=[content_width*0.40, content_width*0.15, content_width*0.15, content_width*0.15, content_width*0.15], hAlign="LEFT")
        t.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), FONT_MAIN),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('GRID', (0, 0), (-1, -1), 0.35, colors.black),
            ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        elements.append(t)
        elements.append(Spacer(1, 10))

    # ✅ add Yesterday + L-7 tables
    snapshot_table("Yesterday", snapshot_yesterday or [])
    snapshot_table("L-7", snapshot_l7 or [])

    # Continue with the normal report sections
    elements += [
        Paragraph("<b>AI Summary – By Rep</b>", styles["H2"]),
        Spacer(1, 6),
    ]


    # ✅ NOW loop and append per-rep paragraphs
    for rep_name in sorted(rep_summaries.keys()):
        txt = rep_summaries.get(rep_name, "No AI summary returned for this rep.")
        elements.append(Paragraph(f"<b>{rep_name}:</b> {txt}", styles["Body"]))
        elements.append(Spacer(1, 6))

    elements.append(Spacer(1, 10))

    # Office Web Usage (keep your existing block)
    elements.append(Paragraph("<b>Office Web Usage</b>", styles["H2"]))
    if web_usage:
        usage_rows = [["Application", "Share (%)"]] + [[app, f"{pct}%"] for app, pct in web_usage]
        uw_total = 0.8 * content_width
        usage_col_widths = [round(uw_total * 0.65, 2), round(uw_total * 0.35, 2)]
        usage_table = Table(usage_rows, colWidths=usage_col_widths, hAlign="LEFT")
        usage_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), FONT_MAIN),
            ('FONTSIZE', (0, 0), (-1, -1), 8.5),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (1, 1), (1, -1), 'RIGHT'),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('GRID', (0, 0), (-1, -1), 0.35, colors.black),
        ]))
        elements.append(usage_table)
    else:
        elements.append(Paragraph("No web usage recorded today.", styles["Body"]))

    doc.build(elements)
    pdf_bytes = buf.getvalue()
    buf.close()
    return filename, pdf_bytes


# ---------- Main ----------
def main(manager_id: int):
    # Still needed for web usage + Pacific date label
    totals_activity, user_activities, user_calls, web_usage, pacific_date_str = fetch_metrics(
        manager_id=manager_id
    )

    # We always run this the next morning → analyze PACIFIC YESTERDAY
    pacific_yesterday_date = (
        datetime.now(timezone("US/Pacific")).date() - timedelta(days=1)
    )

    # ✅ L-7 = 7 days before yesterday (Pacific calendar)
    pacific_l7_date = pacific_yesterday_date - timedelta(days=7)

    # Pull fact_daily rows for YESTERDAY + L-7 (manager-scoped)
    conn = mysql.connector.connect(**MYSQL_CONFIG)

    fact_rows_yesterday = fetch_fact_daily_for_manager(conn, manager_id, pacific_yesterday_date)
    fact_rows_l7        = fetch_fact_daily_for_manager(conn, manager_id, pacific_l7_date)

    # ✅ Index Score maps (from elite_calls_fact_daily)
    index_map_yesterday = fetch_index_scores_for_manager(conn, manager_id, pacific_yesterday_date)
    index_map_l7        = fetch_index_scores_for_manager(conn, manager_id, pacific_l7_date)

    conn.close()



    # AI summaries come ONLY from fact_daily now (yesterday)
    office_summary, rep_summaries = get_ai_summaries(
        fact_rows_yesterday, pacific_date_str
    )

    # ✅ normalize wording so we don't say "talk minutes"
    office_summary = normalize_ai_language(office_summary)

    # ✅ normalize wording for each rep summary too
    for k in list(rep_summaries.keys()):
        rep_summaries[k] = normalize_ai_language(rep_summaries[k])

    # ✅ Build snapshot tables (bucket scores only)
    def build_bucket_rows(rows, index_map):
        out = []
        for r in rows:
            uid = int(r.get("user_id") or 0)

            out.append({
                "name": (r.get("user_name") or r.get("email") or "Unknown"),
                "phone": float(r.get("phone_activity_score") or 0),
                "quote": float(r.get("quote_activity_score") or 0),
                "movement": float(r.get("movement_activity_score") or 0),

                # ✅ new column
                "index_score": float(index_map.get(uid, 0.0)),
            })

        out.sort(key=lambda x: x["name"].lower())
        return out

    # Agent table removed → PDF no longer needs totals / agent_rows
    return generate_pdf_bytes(
        office_summary,
        rep_summaries,
        web_usage,
        pacific_date_str,
        snapshot_yesterday=build_bucket_rows(fact_rows_yesterday, index_map_yesterday),
        snapshot_l7=build_bucket_rows(fact_rows_l7, index_map_l7),
    )
