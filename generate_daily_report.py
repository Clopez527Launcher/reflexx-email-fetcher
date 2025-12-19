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

            -- ✅ CE L7 Z-Scores (needed for the L-7 table in the PDF)
            fd.phone_ce_l7_z,
            fd.quote_ce_l7_z,
            fd.movement_ce_l7_z,


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
        "minimal talk": "minimal talk time",
        "low talk": "low talk time",
        "total talk": "total talk time",
    }

    for bad, good in replacements.items():
        text = text.replace(bad, good)

    return text

def score_to_label(z) -> str:
    """
    Matches your JS cutoffs:
      > 1.5  Excellent
      > 0.5  Above Average
      >= -0.5 Average
      >= -1.5 Below Average
      else   Poor
    """
    try:
        zNum = float(z)
    except Exception:
        return "Average"

    if zNum > 1.5:
        return "Excellent"
    if zNum > 0.5:
        return "Above Average"
    if zNum >= -0.5:
        return "Average"
    if zNum >= -1.5:
        return "Below Average"
    return "Poor"

def talk_minutes_to_phone_label(avg_minutes: float) -> str:
    """
    Your rubric:
      < 30  = Poor
      >=60  = Average
      >=90  = Good
      >=120 = Great

    We'll treat 30-59 as "Below Average".
    """
    m = float(avg_minutes or 0)

    if m >= 120:
        return "Great"
    if m >= 90:
        return "Good"
    if m >= 60:
        return "Average"
    if m < 30:
        return "Poor"
    return "Below Average"


def label_to_pdf_color(label: str) -> str:
    """
    Wraps a label in a ReportLab <font> tag to match dashboard colors.
    """
    if label == "Excellent":
        return '<font color="#00E5FF"><b>Excellent</b></font>'  # cyan
    if label == "Above Average":
        return '<font color="#00C853"><b>Above Average</b></font>'  # green
    if label == "Average":
        return '<font color="#D4AF37"><b>Average</b></font>'  # Gold
    if label == "Below Average":
        return '<font color="#FF9100"><b>Below Average</b></font>'  # orange
    if label == "Poor":
        return '<font color="#FF1744"><b>Poor</b></font>'  # red

    return label

    
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

def get_ai_rep_coaching_l7(coaching_rows, pacific_date_str: str):
    """
    coaching_rows: list of dicts like:
      {
        "display_name": "Nedda Joveini",
        "movement_label": "Poor",
        "quote_label": "Excellent",
        "talk_avg_minutes": 72.5,
        "talk_label": "Average",
        "active_days": 5
      }

    Returns dict[display_name] = 2-sentence coaching summary
    """
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        return {r["display_name"]: "AI coaching unavailable (OPENAI_API_KEY not set)." for r in coaching_rows}

    prompt = f"""
You are Reflexx AI. Write coaching advice using L-7 lookback signals for Pacific date {pacific_date_str}.

Interpretation rules:
- Movement:
  - Below Average or Poor = rep is having trouble navigating programs / workflow friction. Coach them on navigation, shortcuts, process.
  - Above Average or better = no navigation issue flagged.
- Quote:
  - Above Average or Excellent = they are increasing their chances of selling. Reinforce.
  - Below Average or Poor = they are decreasing their chances of selling. Coach quoting behaviors.
- Phone (talk time) uses talk_avg_minutes_per_active_day over last 7 days:
  - < 30 = Poor
  - 30-59 = Below Average
  - 60-89 = Average
  - 90-119 = Good
  - >= 120 = Great
- ALWAYS say "talk time" (not "talk").

DATA (per rep):
{json.dumps(coaching_rows, indent=2)}

Return STRICT JSON only:
{{
  "rep_summaries": [
    {{
      "display_name": "Must match input display_name exactly",
      "summary": "Two sentences. Sentence 1: diagnosis from Movement/Quote/Phone. Sentence 2: specific coaching actions (1-2 concrete things)."
    }}
  ]
}}

Rules:
- Do NOT invent numbers.
- Keep it direct and sales-manager style.
- Must include EVERY rep in DATA.
""".strip()

    from openai import OpenAI
    client = OpenAI(api_key=key)
    r = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    raw = (r.choices[0].message.content or "").strip()

    try:
        data = json.loads(raw)
    except Exception:
        try:
            start = raw.find("{"); end = raw.rfind("}")
            data = json.loads(raw[start:end+1])
        except Exception:
            return {r["display_name"]: "AI coaching error: could not parse response." for r in coaching_rows}

    rep_list = data.get("rep_summaries") or []
    out = {}
    for item in rep_list:
        dn = (item.get("display_name") or "").strip()
        sm = (item.get("summary") or "").strip()
        if dn:
            out[dn] = sm or "No AI coaching returned."

    # Ensure every rep has something
    for r in coaching_rows:
        dn = r["display_name"]
        if dn not in out:
            out[dn] = "No AI coaching returned."

    return out

def fetch_l7_talk_avg_per_active_day(conn, manager_id: int, end_day: date):
    """
    Computes talk time coaching stat:
      avg_talk_minutes_per_active_day over the last 7 calendar days ending at end_day.

    active_day = a day where total_talk_minutes > 0
    returns dict[email_lower] = {
        "avg": float,
        "sum": float,
        "active_days": int
    }
    """
    start_day = end_day - timedelta(days=6)
    cur = conn.cursor(dictionary=True)

    sql = """
        SELECT
            LOWER(u.email) AS email,
            SUM(COALESCE(fd.ib_time_minutes,0) + COALESCE(fd.ob_time_minutes,0)) AS talk_sum,
            SUM(
                CASE
                  WHEN (COALESCE(fd.ib_time_minutes,0) + COALESCE(fd.ob_time_minutes,0)) > 0 THEN 1
                  ELSE 0
                END
            ) AS active_days
        FROM fact_daily fd
        JOIN users u ON u.id = fd.user_id
        WHERE fd.date BETWEEN %s AND %s
          AND (u.manager_id = %s OR u.id = %s)
        GROUP BY LOWER(u.email)
    """
    cur.execute(sql, (start_day, end_day, manager_id, manager_id))

    out = {}
    for r in cur.fetchall():
        talk_sum = float(r.get("talk_sum") or 0)
        active_days = int(r.get("active_days") or 0)
        avg = (talk_sum / active_days) if active_days > 0 else 0.0
        out[(r.get("email") or "").strip()] = {
            "avg": round(avg, 1),
            "sum": round(talk_sum, 1),
            "active_days": active_days
        }
    return out


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
        name="H2Emoji",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",  # ✅ supports ✨ more reliably
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
    from reportlab.lib.enums import TA_CENTER

    styles.add(ParagraphStyle(
        name="ScoreStyle",
        parent=styles["Body"],
        fontName=FONT_BOLD,   # ✅ bold like dashboard
        fontSize=8.5,           # ✅ smaller so it stays 1 line
        leading=9.5,
        alignment=TA_CENTER,  # ✅ centered
        spaceBefore=0,
        spaceAfter=0,
    ))

    # ✅ Build elements FIRST (no loops inside the list)
    elements = [
        Paragraph(f"<b>Report for {pacific_date_str}</b>", styles["ReportTitle"]),
        Spacer(1, 10),

        Paragraph("AI Office Summary", styles["H2Emoji"]),
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

        # We'll keep generic headers here; table title will explain which mode it is
        data = [["Rep", "Phone", "Quote", "Movement"]]

        for r in rows:
            data.append([
                r["name"],
                Paragraph(str(r["phone"]), styles["ScoreStyle"]),
                Paragraph(str(r["quote"]), styles["ScoreStyle"]),
                Paragraph(str(r["movement"]), styles["ScoreStyle"]),
            ])

        tbl = Table(data, colWidths=[220, 95, 95, 95])

        tbl.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), FONT_MAIN),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('GRID', (0, 0), (-1, -1), 0.35, colors.black),

            # ✅ Center the score cells (Phone/Quote/Movement)
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),

            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))

        elements.append(tbl)
        elements.append(Spacer(1, 10))

    # ✅ add Yesterday + L-7 tables
    snapshot_table("Yesterday Z-Scores", snapshot_yesterday or [])
    snapshot_table("L-7 Z-Scores", snapshot_l7 or [])

    # Continue with the normal report sections
    elements += [
        Paragraph("<b>AI Coaching Suggestions</b>", styles["H2"]),
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


def main(manager_id: int):
    totals_activity, user_activities, user_calls, web_usage, pacific_date_str = fetch_metrics(
        manager_id=manager_id
    )

    pacific_yesterday_date = (
        datetime.now(timezone("US/Pacific")).date() - timedelta(days=1)
    )

    conn = mysql.connector.connect(**MYSQL_CONFIG)

    fact_rows_yesterday = fetch_fact_daily_for_manager(conn, manager_id, pacific_yesterday_date)

    talk_l7_map = fetch_l7_talk_avg_per_active_day(conn, manager_id, pacific_yesterday_date)

    index_map_yesterday = fetch_index_scores_for_manager(conn, manager_id, pacific_yesterday_date)

    conn.close()

    office_summary, _rep_summaries_email = get_ai_summaries(
        fact_rows_yesterday, pacific_date_str
    )

    coaching_rows = []
    for r in fact_rows_yesterday:
        email = (r.get("email") or "").strip().lower()
        display_name = (r.get("user_name") or r.get("email") or "Unknown").strip()

        movement_label = score_to_label(r.get("movement_ce_l7_z"))
        quote_label    = score_to_label(r.get("quote_ce_l7_z"))

        talk_stats = talk_l7_map.get(email, {"avg": 0.0, "active_days": 0})
        talk_avg = float(talk_stats.get("avg") or 0.0)
        active_days = int(talk_stats.get("active_days") or 0)

        coaching_rows.append({
            "display_name": display_name,
            "movement_label": movement_label,
            "quote_label": quote_label,
            "talk_avg_minutes_per_active_day": talk_avg,
            "talk_time_label": talk_minutes_to_phone_label(talk_avg),
            "active_days": active_days
        })

    rep_summaries = get_ai_rep_coaching_l7(coaching_rows, pacific_date_str)

    office_summary = normalize_ai_language(office_summary)
    for k in list(rep_summaries.keys()):
        rep_summaries[k] = normalize_ai_language(rep_summaries[k])

    def build_bucket_rows(rows, index_map, mode="bucket"):
        out = []
        for r in rows:
            uid = int(r.get("user_id") or 0)

            if mode == "l7z":
                phone_val    = float(r.get("phone_ce_l7_z") or 0)
                quote_val    = float(r.get("quote_ce_l7_z") or 0)
                movement_val = float(r.get("movement_ce_l7_z") or 0)
            else:
                phone_val    = float(r.get("phone_activity_score") or 0)
                quote_val    = float(r.get("quote_activity_score") or 0)
                movement_val = float(r.get("movement_activity_score") or 0)

            out.append({
                "name": (r.get("user_name") or r.get("email") or "Unknown"),
                "phone": label_to_pdf_color(score_to_label(phone_val)),
                "quote": label_to_pdf_color(score_to_label(quote_val)),
                "movement": label_to_pdf_color(score_to_label(movement_val)),
                "index_score": float(index_map.get(uid, 0.0)),
            })

        out.sort(key=lambda x: float(x.get("index_score", 0.0)), reverse=True)
        return out

    return generate_pdf_bytes(
        office_summary,
        rep_summaries,
        web_usage,
        pacific_date_str,
        snapshot_yesterday=build_bucket_rows(fact_rows_yesterday, index_map_yesterday, mode="bucket"),
        snapshot_l7=build_bucket_rows(fact_rows_yesterday, index_map_yesterday, mode="l7z"),
    )

