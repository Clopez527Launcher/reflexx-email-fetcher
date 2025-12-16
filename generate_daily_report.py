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
MYSQL_CONFIG = {
    "host": "mysql.railway.internal",
    "user": "root",
    "password": os.getenv("vbNVbSKVuUvYRJzhewpufAXbxcatfKIc") or os.getenv("vbNVbSKVuUvYRJzhewpufAXbxcatfKIc"),
    "database": "railway",
    "port": 3306
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


# ---------- ✅ Local scorecard logic (no imports) ----------
def calculate_scorecard_from_raw(distance, keys, clicks, idle_count):
    """
    Mirrors your grading matrix in the PDF:
      - Mouse Distance: 100/200/300/400 -> 5/10/15/20
      - Keystrokes: 4000/6000/8000/10000 -> 5/10/15/20
      - Mouse Clicks: 1000/1500/2000/2500 -> 5/10/15/20
      - Idle Count: <=120/90/60/30 -> 10/20/30/40
    Total max = 100
    """
    d = safe_int(distance)
    k = safe_int(keys)
    c = safe_int(clicks)
    idle = safe_int(idle_count)

    def tier_points(val, tiers):
        # tiers is list of (threshold, points) in ascending threshold order
        pts = 0
        for threshold, p in tiers:
            if val >= threshold:
                pts = p
        return pts

    dist_pts = tier_points(d, [(100,5),(200,10),(300,15),(400,20)])
    key_pts  = tier_points(k, [(4000,5),(6000,10),(8000,15),(10000,20)])
    click_pts= tier_points(c, [(1000,5),(1500,10),(2000,15),(2500,20)])

    # idle is reversed (lower is better)
    if idle <= 30:
        idle_pts = 40
    elif idle <= 60:
        idle_pts = 30
    elif idle <= 90:
        idle_pts = 20
    elif idle <= 120:
        idle_pts = 10
    else:
        idle_pts = 0

    score = dist_pts + key_pts + click_pts + idle_pts

    if score >= 90:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 70:
        grade = "C"
    elif score >= 60:
        grade = "D"
    else:
        grade = "F"

    return {"score": score, "grade": grade}


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


# ---------- AI summary (safe fallback) ----------
def get_ai_summary(agent_data):
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        return "Summary unavailable (OPENAI_API_KEY not set)."

    prompt = "You are Reflexx AI. Here's the office performance for today:\n\n"
    for row in agent_data:
        name, inbound, outbound, in_talk, out_talk = row[:5]
        prompt += f"- {name}: Inbound {inbound}, Outbound {outbound}, In Talk {in_talk}, Out Talk {out_talk}\n"
    prompt += "\nPlease summarize the day's performance in one paragraph and give one improvement suggestion."

    from openai import OpenAI
    client = OpenAI(api_key=key)
    r = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return r.choices[0].message.content


def grade_color_html(grade_letter: str, score: int) -> str:
    g = (grade_letter or "F").upper()[:1]
    if g in ("F", "D"):
        color = "#d32f2f"
    elif g == "C":
        color = "#ef6c00"
    elif g == "B":
        color = "#f9a825"
    else:
        color = "#2e7d32"  # A
    return f'<font color="{color}"><b>{g}</b> ({score}%)</font>'


# ---------- PDF generation (returns BYTES) ----------
def generate_pdf_bytes(summary, totals, agent_rows, web_usage, pacific_date_str: str):
    pacific = timezone("US/Pacific")
    now = datetime.now(pacific)
    timestamp = now.strftime('%b %d, %Y at %I:%M %p')
    filename = f"Reflexx Daily Report – {timestamp}.pdf"

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=LETTER, leftMargin=54, rightMargin=54, topMargin=54, bottomMargin=54)
    content_width = doc.width

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="ReportTitle", parent=styles["Title"], fontName=FONT_MAIN, fontSize=18, leading=22))
    styles.add(ParagraphStyle(name="Body", parent=styles["BodyText"], fontName=FONT_MAIN, fontSize=10.25, leading=14))
    styles.add(ParagraphStyle(name="H2", parent=styles["Heading2"], fontName=FONT_MAIN, fontSize=13, leading=16))
    styles.add(ParagraphStyle(name="HeaderWhiteSmall", fontName=FONT_BOLD, fontSize=7.0, leading=8.6, alignment=1, textColor=colors.white))

    elements = [
        Paragraph(f"<b>Reflexx Daily Report – {timestamp}</b>", styles['ReportTitle']),
        Spacer(1, 6),
        Paragraph(f"<i>Data window: Pacific calendar day {pacific_date_str}</i>", styles['Body']),
        Spacer(1, 10),
        Paragraph(summary, styles['Body']),
        Spacer(1, 14)
    ]

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
        ('LEFTPADDING', (0,0), (-1,-1), 2),
        ('RIGHTPADDING', (0,0), (-1,-1), 2),
        ('TOPPADDING', (0,0), (-1,-1), 2),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
        ('ALIGN', (1, 1), (8, -1), 'CENTER'),
        ('ALIGN', (0, 1), (0, -1), 'LEFT'),
        ('ALIGN', (9, 1), (9, -1), 'CENTER'),
        ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.35, colors.black),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.whitesmoke]),
    ]))
    elements.extend([agent_table, Spacer(1, 14)])

    # Office Web Usage
    elements.append(Paragraph("<b>Office Web Usage</b>", styles['H2']))
    if web_usage:
        usage_rows = [["Application", "Share (%)"]] + [[app, f"{pct}%"] for app, pct in web_usage]
        uw_total = 0.8 * content_width
        usage_col_widths = [round(uw_total * 0.65, 2), round(uw_total * 0.35, 2)]
        usage_table = Table(usage_rows, colWidths=usage_col_widths, hAlign="LEFT")
        usage_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), FONT_MAIN),
            ('FONTSIZE', (0, 0), (-1, -1), 8.5),
            ('LEFTPADDING', (0,0), (-1,-1), 3),
            ('RIGHTPADDING', (0,0), (-1,-1), 3),
            ('TOPPADDING', (0,0), (-1,-1), 2),
            ('BOTTOMPADDING', (0,0), (-1,-1), 2),
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (1, 1), (1, -1), 'RIGHT'),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('GRID', (0, 0), (-1, -1), 0.35, colors.black),
        ]))
        elements.append(usage_table)
    else:
        elements.append(Paragraph("No web usage recorded today.", styles['Body']))

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

        card = calculate_scorecard_from_raw(distance, keys, clicks, idle)
        grade = card["grade"]
        score = card["score"]
        grade_cell = Paragraph(grade_color_html(grade, score), getSampleStyleSheet()["BodyText"])

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

    summary = get_ai_summary(ai_input)
    return generate_pdf_bytes(summary, total_row, agent_rows, web_usage, pacific_date_str)

