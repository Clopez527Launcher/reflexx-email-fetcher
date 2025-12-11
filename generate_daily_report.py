import os
import openai
import mysql.connector
from datetime import datetime, date, timedelta
from reportlab.lib.pagesizes import LETTER
from reportlab.platypus import (
    Table, TableStyle, SimpleDocTemplate, Paragraph, Spacer
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from pytz import timezone, utc
from scorecard_api import calculate_scorecard_from_raw

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
    "password": "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc",
    "database": "railway",
    "port": 3306
}

openai.api_key = os.getenv("OPENAI_API_KEY")
PDF_DIR = "saved_reports"
os.makedirs(PDF_DIR, exist_ok=True)

# ---------- Helpers ----------
def pacific_day_utc_window(target_local_date: date):
    """Return (start_utc, end_utc, pacific_date_str) for the given Pacific calendar date."""
    pac = timezone("US/Pacific")
    start_local = pac.localize(datetime(target_local_date.year, target_local_date.month, target_local_date.day, 0, 0, 0, 0))
    end_local   = pac.localize(datetime(target_local_date.year, target_local_date.month, target_local_date.day, 23, 59, 59, 999999))
    return start_local.astimezone(utc), end_local.astimezone(utc), target_local_date.strftime("%Y-%m-%d")

def hms_to_secs(s: str) -> int:
    if not s or s == "0:00:00": return 0
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

# ---------- Save generated PDF ----------
def save_report_to_db(filename, filepath, manager_id):
    with open(filepath, "rb") as f:
        binary_data = f.read()
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO reports (filename, file_data, manager_id) VALUES (%s, %s, %s)",
        (filename, binary_data, manager_id)
    )
    conn.commit()
    cur.close(); conn.close()

# ---------- Fetch metrics (TZ-correct) ----------
def fetch_metrics(pacific_date: date = None):
    """
    - activity_log (UTC timestamps) using Pacific-day UTC window
    - call_metrics per user by Pacific DATE (report_date)
    """
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    if pacific_date is None:
        pacific_date = datetime.now(timezone("US/Pacific")).date()

    start_utc, end_utc, pacific_date_str = pacific_day_utc_window(pacific_date)
    print(f"🔍 activity_log window {start_utc} → {end_utc} UTC (Pacific day {pacific_date_str})")

    # Office totals from activity_log (UTC window)
    cur.execute("""
        SELECT SUM(mouse_distance), SUM(keystrokes), SUM(mouse_clicks), SUM(idle_count)
        FROM activity_log
        WHERE timestamp BETWEEN %s AND %s
    """, (start_utc, end_utc))
    total_activity = cur.fetchone()

    # Per-user activity (UTC window)
    cur.execute("""
        SELECT u.email, SUM(a.mouse_distance), SUM(a.keystrokes), SUM(a.mouse_clicks), SUM(a.idle_count)
        FROM activity_log a
        JOIN users u ON a.user_id = u.id
        WHERE a.timestamp BETWEEN %s AND %s
        GROUP BY u.email
    """, (start_utc, end_utc))
    user_activities = cur.fetchall()

    # Per-user call metrics by Pacific date (DATE column)
    cur.execute("""
        SELECT u.email,
               cm.inbound_calls,
               cm.outbound_calls,
               cm.inbound_time,
               cm.outbound_time
        FROM call_metrics cm
        JOIN users u ON cm.user_id = u.id
        WHERE cm.report_date = %s
    """, (pacific_date_str,))
    user_calls = {row[0]: row[1:] for row in cur.fetchall()}

    # (We keep this query around if you still want to inspect raw DB totals.)
    cur.execute("""
        SELECT 
            SUM(inbound_calls),
            SUM(outbound_calls),
            SEC_TO_TIME(SUM(TIME_TO_SEC(inbound_time))),
            SEC_TO_TIME(SUM(TIME_TO_SEC(outbound_time)))
        FROM call_metrics
        WHERE report_date = %s
    """, (pacific_date_str,))
    total_calls_raw = cur.fetchone()  # not used for totals row anymore (see # TZ below)

    # Web usage from JSON (UTC window)
    cur.execute("""
        SELECT page_time
        FROM activity_log
        WHERE timestamp BETWEEN %s AND %s
    """, (start_utc, end_utc))

    from collections import defaultdict
    import json, decimal
    def safe_get(v):
        try: return float(v) if v is not None else 0.0
        except (decimal.InvalidOperation, ValueError, TypeError): return 0.0

    app_totals = defaultdict(float)
    for (blob,) in cur.fetchall():
        if blob:
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
    return total_activity, user_activities, user_calls, total_calls_raw, web_usage, pacific_date_str

# ---------- AI summary ----------
def get_ai_summary(agent_data):
    prompt = "You are Reflexx AI. Here's the office performance for today:\n\n"
    for row in agent_data:
        name, inbound, outbound, in_talk, out_talk = row[:5]
        prompt += f"- {name}: Inbound {inbound}, Outbound {outbound}, In Talk {in_talk}, Out Talk {out_talk}\n"
    prompt += "\nPlease summarize the day's performance in one paragraph and give one improvement suggestion."
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    r = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return r.choices[0].message.content

# ---------- Grade color helper ----------
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

# ---------- PDF generation ----------
def generate_pdf(summary, totals, agent_rows, web_usage, pacific_date_str: str):
    pacific = timezone("US/Pacific")
    now = datetime.now(pacific)
    timestamp = now.strftime('%b %d, %Y at %I:%M %p')
    filename = f"Reflexx Daily Report – {timestamp}.pdf"
    pdf_path = os.path.join(PDF_DIR, filename)

    LEFT_RIGHT = 54  # 0.75"
    TOP_BOTTOM = 54
    doc = SimpleDocTemplate(
        pdf_path, pagesize=LETTER,
        leftMargin=LEFT_RIGHT, rightMargin=LEFT_RIGHT,
        topMargin=TOP_BOTTOM, bottomMargin=TOP_BOTTOM
    )
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

    # Agent table
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

    # Scorecard Grading (narrow + column colors)
    RED_LT    = colors.HexColor("#fdecea")
    ORANGE_LT = colors.HexColor("#fff4e5")
    YELLOW_LT = colors.HexColor("#fffbe6")
    GREEN_LT  = colors.HexColor("#e8f5e9")

    matrix_data = [
        ["Scorecard Grading", "", "", "", ""],
        ["Mouse Distance", "≥100",  "≥200",  "≥300",   "≥400"],
        ["Points",         "5",     "10",    "15",     "20"],
        ["Keystrokes",     "≥4000", "≥6000", "≥8000",  "≥10000"],
        ["Points",         "5",     "10",    "15",     "20"],
        ["Mouse Clicks",   "≥1000", "≥1500", "≥2000",  "≥2500"],
        ["Points",         "5",     "10",    "15",     "20"],
        ["Idle Count",     "≤120",  "≤90",   "≤60",    "≤30"],
        ["Points",         "10",    "20",    "30",     "40"]
    ]

    matrix_total_width = 0.70 * content_width
    m_first = 0.44
    m_rest = (1 - m_first) / 4.0
    matrix_col_widths = [
        round(matrix_total_width * m_first, 2),
        round(matrix_total_width * m_rest, 2),
        round(matrix_total_width * m_rest, 2),
        round(matrix_total_width * m_rest, 2),
        round(matrix_total_width * m_rest, 2),
    ]

    matrix_table = Table(matrix_data, hAlign="LEFT", colWidths=matrix_col_widths, repeatRows=1)
    matrix_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), FONT_MAIN),
        ('FONTSIZE', (0, 0), (-1, -1), 8.5),
        ('LEFTPADDING', (0,0), (-1,-1), 3),
        ('RIGHTPADDING', (0,0), (-1,-1), 3),
        ('TOPPADDING', (0,0), (-1,-1), 2),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
        ('SPAN', (0, 0), (-1, 0)),
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.35, colors.black),
    ]))
    for col, bg in zip([1, 2, 3, 4], [RED_LT, ORANGE_LT, YELLOW_LT, GREEN_LT]):
        matrix_table.setStyle(TableStyle([('BACKGROUND', (col, 1), (col, 8), bg)]))

    elements.append(matrix_table)
    elements.append(Spacer(1, 12))

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
    return pdf_path

# ---------- Main ----------
def main(manager_id):
    totals_activity, user_activities, user_calls, _totals_calls_raw, web_usage, pacific_date_str = fetch_metrics()

    # Build agent rows and color-coded grades
    agent_rows = []
    ai_input = []

    # TZ: sum totals from the same per-user call set we fetched (Pacific-filtered)
    tot_in = tot_out = 0
    tot_in_secs = tot_out_secs = 0

    for user in user_activities:
        name, distance, keys, clicks, idle = user
        inbound, outbound, in_talk, out_talk = user_calls.get(name, (0, 0, "0:00:00", "0:00:00"))

        # accumulate totals from per-user calls (Pacific day)
        try:   tot_in  += int(inbound or 0)
        except: pass
        try:   tot_out += int(outbound or 0)
        except: pass
        tot_in_secs  += hms_to_secs(in_talk)
        tot_out_secs += hms_to_secs(out_talk)

        card = calculate_scorecard_from_raw(distance, keys, clicks, idle)
        grade = card["grade"]; score = card["score"]
        grade_cell = Paragraph(grade_color_html(grade, score), getSampleStyleSheet()["BodyText"])

        row = [
            name,
            str(inbound), str(outbound), in_talk, out_talk,
            str(int(distance or 0)), str(int(keys or 0)), str(int(clicks or 0)), str(idle or 0),
            grade_cell
        ]
        agent_rows.append(row)
        ai_input.append([name, str(inbound), str(outbound), in_talk, out_talk])

    # TZ: totals row uses Pacific-filtered sums (not the raw DB SUM for the date)
    total_row = [
        str(tot_in),
        str(tot_out),
        secs_to_hms(tot_in_secs),
        secs_to_hms(tot_out_secs),
        str(int(totals_activity[0] or 0)),
        str(int(totals_activity[1] or 0)),
        str(int(totals_activity[2] or 0)),
        str(totals_activity[3] or 0)
    ]

    summary = get_ai_summary(ai_input)
    pdf_path = generate_pdf(summary, total_row, agent_rows, web_usage, pacific_date_str)
    save_report_to_db(os.path.basename(pdf_path), pdf_path, manager_id)
    print(f"✅ Report saved for manager {manager_id}: {pdf_path}")

if __name__ == "__main__":
    main(manager_id=4)  # replace with real manager_id for manual testing
