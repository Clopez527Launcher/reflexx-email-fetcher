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


def safe_int(x):
    try:
        return int(x or 0)
    except Exception:
        return 0


# ---------- AI summaries (office + per rep) ----------
def get_ai_summaries(agent_data, pacific_date_str: str):
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
        reps = {row[0]: office for row in agent_data}
        return office, reps

    # Ask for STRICT JSON so we can render cleanly
    payload = []
    for row in agent_data:
        name, inbound, outbound, in_talk, out_talk = row[:5]
        payload.append({
            "name": name,
            "inbound": inbound,
            "outbound": outbound,
            "in_talk": in_talk,
            "out_talk": out_talk
        })

    prompt = f"""
You are Reflexx AI. Analyze yesterday's performance for the office (Pacific date {pacific_date_str}).

DATA (per rep):
{json.dumps(payload, indent=2)}

Return STRICT JSON only with this exact shape:
{{
  "office_summary": "THREE short sentences about the office overall. Mention who did well and who struggled (based only on the data).",
  "rep_summaries": [
    {{
      "name": "Rep Name",
      "summary": "TWO short sentences about this rep. 1) what they did well, 2) what to improve next."
    }}
  ]
}}

Rules:
- Do NOT invent numbers or facts not in the data.
- Keep it short, direct, and sales-manager style.
- If a metric is zero/low, say it plainly.
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
            reps = {row[0]: "AI summary error: could not parse response." for row in agent_data}
            return office, reps

    office_summary = (data.get("office_summary") or "").strip() or "No office summary returned."
    rep_summaries_list = data.get("rep_summaries") or []

    rep_map = {}
    for item in rep_summaries_list:
        n = (item.get("name") or "").strip()
        s = (item.get("summary") or "").strip()
        if n:
            rep_map[n] = s

    # ensure every rep has *something*
    for row in agent_data:
        name = row[0]
        if name not in rep_map:
            rep_map[name] = "No AI summary returned for this rep."

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
def generate_pdf_bytes(office_summary, rep_summaries, totals, agent_rows, web_usage, pacific_date_str: str):
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

        Paragraph("<b>AI Summary – By Rep</b>", styles["H2"]),
        Spacer(1, 6),
    ]

    # ✅ NOW loop and append per-rep paragraphs
    for rep_name in [r[0] for r in agent_rows]:
        txt = rep_summaries.get(rep_name, "No AI summary returned for this rep.")
        elements.append(Paragraph(f"<b>{rep_name}:</b> {txt}", styles["Body"]))
        elements.append(Spacer(1, 6))

    elements.append(Spacer(1, 10))

    # ---- Agent table (same as your existing code) ----
    header_labels = ["Agent", "Inbound", "Outbound", "In Talk", "Out Talk",
                     "Distance", "Keystrokes", "Clicks", "Idle", "Grade (Score)"]
    headers = [Paragraph(h, styles["HeaderWhiteSmall"]) for h in header_labels]

    col_widths = [
        0.225 * content_width,
        0.070 * content_width,
        0.075 * content_width,
        0.100 * content_width,
        0.100 * content_width,
        0.080 * content_width,
        0.100 * content_width,
        0.090 * content_width,
        0.070 * content_width,
        0.085 * content_width
    ]

    table_data = [headers, ["TOTAL"] + totals] + agent_rows
    agent_table = Table(table_data, repeatRows=1, colWidths=col_widths)
    agent_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
        ('FONTNAME', (0, 1), (-1, -1), FONT_MAIN),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('LEFTPADDING', (0, 0), (-1, -1), 2),
        ('RIGHTPADDING', (0, 0), (-1, -1), 2),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ('ALIGN', (1, 1), (8, -1), 'CENTER'),
        ('ALIGN', (0, 1), (0, -1), 'LEFT'),
        ('ALIGN', (9, 1), (9, -1), 'CENTER'),
        ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.35, colors.black),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.whitesmoke]),
    ]))
    elements.extend([agent_table, Spacer(1, 14)])

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
    totals_activity, user_activities, user_calls, web_usage, pacific_date_str = fetch_metrics(manager_id=manager_id)

    agent_rows = []
    ai_input = []

    tot_in = tot_out = 0
    tot_in_secs = tot_out_secs = 0

    for user in user_activities:
        name, distance, keys, clicks, idle = user
        inbound, outbound, in_talk, out_talk = user_calls.get(name, (0, 0, "0:00:00", "0:00:00"))

        tot_in += safe_int(inbound)
        tot_out += safe_int(outbound)
        tot_in_secs += hms_to_secs(in_talk)
        tot_out_secs += hms_to_secs(out_talk)

        # ✅ Scorecard removed — keep PDF stable
        grade_cell = "-"


        agent_rows.append([
            name,
            str(inbound), str(outbound), in_talk, out_talk,
            str(safe_int(distance)), str(safe_int(keys)), str(safe_int(clicks)), str(safe_int(idle)),
            grade_cell
        ])
        ai_input.append([name, str(inbound), str(outbound), in_talk, out_talk])

    total_row = [
        str(tot_in),
        str(tot_out),
        secs_to_hms(tot_in_secs),
        secs_to_hms(tot_out_secs),
        str(safe_int(totals_activity[0])),
        str(safe_int(totals_activity[1])),
        str(safe_int(totals_activity[2])),
        str(safe_int(totals_activity[3]))
    ]

    office_summary, rep_summaries = get_ai_summaries(ai_input, pacific_date_str)
    return generate_pdf_bytes(office_summary, rep_summaries, total_row, agent_rows, web_usage, pacific_date_str)

