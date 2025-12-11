# ai_routes.py
from flask import Blueprint, request, jsonify, session
from flask_login import login_required
import os, re
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from openai import OpenAI
import mysql.connector

ai_bp = Blueprint('ai_bp', __name__)

# ---------- Timezone constants ----------
PT  = ZoneInfo("America/Los_Angeles")
UTC = ZoneInfo("UTC")

# ---------- DB connection ----------
def get_db_connection():
    return mysql.connector.connect(
        host="mysql.railway.internal",
        user="root",
        password="vbNVbSKVuUvYRJzhewpufAXbxcatfKIc",
        database="railway",
        port=3306
    )

# ---------- Intent helpers ----------
DOW_NAMES = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

def _wants_weekly_dow_totals(q: str) -> bool:
    ql = str(q).lower()
    # Fire only when a weekly, per-week breakdown is clearly requested
    return bool(re.search(
        r"(each|per|by)\s+week|weekly\b|week\s*by\s*week|for\s+all\s+weeks|all\s+four\s+weeks",
        ql
    ))

def _wants_compare_previous(q: str) -> bool:
    ql = str(q).lower()
    return bool(re.search(r"\b(compare|vs|versus|week before|previous week|prior week|previous month|prior month)\b", ql))

def _wants_day_of_week(q: str) -> bool:
    ql = str(q).lower()
    # “what day of week…”, “which day…”, “weekday…”, “best day…”
    return bool(re.search(r"(day\s*of\s*week|which\s+day|what\s+day|weekday|best\s+day)", ql))

def _wants_outbound(q: str):
    ql = str(q).lower()
    return bool(re.search(r"\boutbound(?:s)?\b|\bout[-\s]?calls?\b|\bdials?\b", ql))

def _wants_inbound(q: str):
    ql = str(q).lower()
    return bool(re.search(r"\binbound(?:s)?\b|\bin[-\s]?calls?\b", ql))

def _wants_calls(q: str):
    ql = str(q).lower()
    return bool(re.search(r"\bcalls?\b|\bdials?\b|\bcall\s+volume\b|\btotals?\b", ql))

def _wants_talk_time(q: str):
    ql = str(q).lower()
    return bool(re.search(r"\btalk(?:\s|-)?time\b|\bminutes\b|\bhours\b|\bhandle\s*time\b", ql))

def _extract_extension(q):
    m = re.search(r"ext(?:ension)?\s*(\d{3,8})", str(q).lower())
    return m.group(1) if m else None

# ---------- Small utils ----------
def _start_of_week(d: date):
    # Monday start; change if you prefer Sunday
    return d - timedelta(days=d.weekday())

def _month_bounds(year: int, month: int):
    s = date(year, month, 1)
    e = date(year + (month == 12), (month % 12) + 1, 1)
    return s, e

def _prev_period(start_utc: datetime, end_utc: datetime):
    """Return the immediately prior period of equal length."""
    span = end_utc - start_utc
    return (start_utc - span, end_utc - span)

def _pct_change(curr: int, prev: int) -> str:
    if prev == 0:
        return "—"
    return f"{((curr - prev) / prev) * 100:.1f}%"

def _label_dates_pt(start_utc: datetime, end_utc: datetime) -> str:
    # end_utc is exclusive; show inclusive end-1 day in PT
    s = start_utc.astimezone(PT).date()
    e = (end_utc - timedelta(days=1)).astimezone(PT).date()
    return f"{s}–{e}"

def _fmt_hms(seconds):
    seconds = int(seconds or 0)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    parts = []
    if h: parts.append(f"{h}h")
    if m or (h and s): parts.append(f"{m}m")
    if s and not h: parts.append(f"{s}s")
    return " ".join(parts) or "0m"

# ---------- Natural-language range parser ----------
def _parse_range_natural(q, now_dt=None):
    """
    Returns (start_utc_naive, end_utc_naive, label) or (None, None, None).
    If now_dt is provided, it should be a timezone-aware PT datetime.
    """
    if not q:
        return None, None, None

    ql = str(q).lower().strip()
    now_dt = now_dt or datetime.now(PT)
    today_d = now_dt.date()

    WEEKDAYS = {
        "monday": 0, "tuesday": 1, "wednesday": 2,
        "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6
    }

    def _pt_day_range(d):
        start_pt = datetime.combine(d, datetime.min.time(), PT)
        end_pt = start_pt + timedelta(days=1)
        return (start_pt.astimezone(UTC).replace(tzinfo=None),
                end_pt.astimezone(UTC).replace(tzinfo=None))

    # last monday
    m = re.search(r"(?:on|at)?\s*last\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", ql)
    if m:
        wd = WEEKDAYS[m.group(1)]
        delta = (today_d.weekday() - wd) % 7 or 7
        target = today_d - timedelta(days=delta)
        s_utc, e_utc = _pt_day_range(target)
        return s_utc, e_utc, f"{target.strftime('%A %Y-%m-%d')}"

    # this monday
    m = re.search(r"(?:on|at)?\s*this\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", ql)
    if m:
        wd = WEEKDAYS[m.group(1)]
        sow = _start_of_week(today_d)
        target = sow + timedelta(days=wd)
        s_utc, e_utc = _pt_day_range(target)
        return s_utc, e_utc, f"{target.strftime('%A %Y-%m-%d')}"

    # bare monday (most recent <= today)
    m = re.search(r"(?:on|at)?\s*(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", ql)
    if m:
        wd = WEEKDAYS[m.group(1)]
        delta = (today_d.weekday() - wd) % 7
        target = today_d - timedelta(days=delta)
        s_utc, e_utc = _pt_day_range(target)
        return s_utc, e_utc, f"{target.strftime('%A %Y-%m-%d')}"

    # last week
    if re.search(r"\b(last|previous|prior)\s+week\b|\blast\s*wk\b", ql):
        this_week_start = _start_of_week(today_d)
        last_week_start = this_week_start - timedelta(days=7)
        start_pt = datetime.combine(last_week_start, datetime.min.time(), PT)
        end_pt = datetime.combine(this_week_start, datetime.min.time(), PT)
        return (start_pt.astimezone(UTC).replace(tzinfo=None),
                end_pt.astimezone(UTC).replace(tzinfo=None),
                "last week")

    # last month
    if re.search(r"\b(last|previous|prior)\s+month\b", ql):
        y, mth = today_d.year, today_d.month
        if mth == 1:
            y, mth = y - 1, 12
        else:
            mth -= 1
        s, e = _month_bounds(y, mth)
        start_pt = datetime.combine(s, datetime.min.time(), PT)
        end_pt = datetime.combine(e, datetime.min.time(), PT)
        return (start_pt.astimezone(UTC).replace(tzinfo=None),
                end_pt.astimezone(UTC).replace(tzinfo=None),
                "last month")

    # today
    if "today" in ql:
        start_pt = datetime.combine(today_d, datetime.min.time(), PT)
        end_pt = start_pt + timedelta(days=1)
        return (start_pt.astimezone(UTC).replace(tzinfo=None),
                end_pt.astimezone(UTC).replace(tzinfo=None),
                "today")

    # yesterday
    if "yesterday" in ql:
        y = today_d - timedelta(days=1)
        start_pt = datetime.combine(y, datetime.min.time(), PT)
        end_pt = start_pt + timedelta(days=1)
        return (start_pt.astimezone(UTC).replace(tzinfo=None),
                end_pt.astimezone(UTC).replace(tzinfo=None),
                "yesterday")

    # past N days
    m = re.search(r"past (\d+)\s*days?\b", ql)
    if m:
        n = int(m.group(1))
        anchor_start = datetime.combine(today_d, datetime.min.time(), PT)
        start_pt = anchor_start - timedelta(days=n)
        end_pt = anchor_start + timedelta(days=1)  # include anchor day
        return (start_pt.astimezone(UTC).replace(tzinfo=None),
                end_pt.astimezone(UTC).replace(tzinfo=None),
                f"past {n} days")

    # "july 2025", etc.
    m = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+(\d{4})", ql)
    if m:
        mon_map = dict(jan=1,feb=2,mar=3,apr=4,may=5,jun=6,jul=7,aug=8,sep=9,sept=9,oct=10,nov=11,dec=12)
        month = mon_map[m.group(1)[:3]]
        year = int(m.group(2))
        s, e = _month_bounds(year, month)
        start_pt = datetime.combine(s, datetime.min.time(), PT)
        end_pt = datetime.combine(e, datetime.min.time(), PT)
        return (start_pt.astimezone(UTC).replace(tzinfo=None),
                end_pt.astimezone(UTC).replace(tzinfo=None),
                f"{s.strftime('%B %Y')}")

    return None, None, None

# ---------- Query helpers (match dashboard de-dupe) ----------
def _query_calls_range(conn, manager_id, start_utc, end_utc, direction=None, user_id=None, extension=None):
    """
    EXACTLY match the dashboard tile:
    - local_date := DATE(CONVERT_TZ(created_at,'UTC','America/Los_Angeles'))
    - latest row per (user_id, local_date) by created_at (rn = 1)
    - sum those rows
    - filter by users.manager_id
    - DO NOT filter out NULL extensions
    """
    start_date_pt = start_utc.astimezone(PT).date()
    end_date_pt   = (end_utc - timedelta(seconds=1)).astimezone(PT).date()

    emp_filter_sql = ""
    params = [manager_id]
    if user_id:
        emp_filter_sql = "AND cm.user_id = %s"
        params.append(user_id)
    if extension:
        emp_filter_sql += " AND cm.extension = %s"
        params.append(extension)

    sql = f"""
        WITH base AS (
            SELECT
              cm.*,
              DATE(CONVERT_TZ(cm.created_at, 'UTC', 'America/Los_Angeles')) AS local_date
            FROM call_metrics cm
            JOIN users u ON u.id = cm.user_id
            WHERE u.manager_id = %s
              {emp_filter_sql}
        ),
        ranked AS (
            SELECT
              base.*,
              ROW_NUMBER() OVER (
                PARTITION BY base.user_id, base.local_date
                ORDER BY base.created_at DESC
              ) AS rn
            FROM base
        )
        SELECT
          COALESCE(SUM(inbound_calls), 0)                      AS inbound_calls,
          COALESCE(SUM(outbound_calls), 0)                     AS outbound_calls,
          COALESCE(SUM(total_calls), 0)                        AS total_calls,
          COALESCE(SUM(TIME_TO_SEC(inbound_time)), 0)          AS inbound_talk_seconds,
          COALESCE(SUM(TIME_TO_SEC(outbound_time)), 0)         AS outbound_talk_seconds,
          COALESCE(SUM(TIME_TO_SEC(handle_time)), 0)           AS handle_seconds
        FROM ranked
        WHERE rn = 1
          AND local_date BETWEEN %s AND %s;
    """

    params += [start_date_pt, end_date_pt]

    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params)
    row = cur.fetchone() or {}
    cur.close()

    for k in ("inbound_calls","outbound_calls","total_calls",
              "inbound_talk_seconds","outbound_talk_seconds","handle_seconds"):
        row[k] = int(row.get(k) or 0)
    return row

def _breakdown_by_date(conn, manager_id, start_utc, end_utc, user_id=None, extension=None):
    """
    Returns rows: {local_date, inbound_calls, outbound_calls, total_calls, ...}
    using the SAME latest-row-per-user/day rule the dashboard uses.
    """
    start_date_pt = start_utc.astimezone(PT).date()
    end_date_pt   = (end_utc - timedelta(seconds=1)).astimezone(PT).date()

    emp_filter_sql = ""
    params = [manager_id]
    if user_id:
        emp_filter_sql = "AND cm.user_id = %s"
        params.append(user_id)
    if extension:
        emp_filter_sql += " AND cm.extension = %s"
        params.append(extension)

    sql = f"""
        WITH base AS (
            SELECT
              cm.*,
              DATE(CONVERT_TZ(cm.created_at, 'UTC', 'America/Los_Angeles')) AS local_date
            FROM call_metrics cm
            JOIN users u ON u.id = cm.user_id
            WHERE u.manager_id = %s
              {emp_filter_sql}
        ),
        ranked AS (
            SELECT
              base.*,
              ROW_NUMBER() OVER (
                PARTITION BY base.user_id, base.local_date
                ORDER BY base.created_at DESC
              ) AS rn
            FROM base
        )
        SELECT
          local_date,
          SUM(inbound_calls)                 AS inbound_calls,
          SUM(outbound_calls)                AS outbound_calls,
          SUM(total_calls)                   AS total_calls,
          SUM(TIME_TO_SEC(inbound_time))     AS inbound_talk_seconds,
          SUM(TIME_TO_SEC(outbound_time))    AS outbound_talk_seconds,
          SUM(TIME_TO_SEC(handle_time))      AS handle_seconds
        FROM ranked
        WHERE rn = 1
          AND local_date BETWEEN %s AND %s
        GROUP BY local_date
        ORDER BY local_date;
    """
    params += [start_date_pt, end_date_pt]

    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params)
    rows = cur.fetchall() or []
    cur.close()
    return rows

def _weekly_dow_totals(conn, manager_id, start_utc, end_utc, user_id=None, extension=None):
    """
    Return weekly buckets (Mon–Sun in PT) with per-weekday totals.
    Only weekdays that actually occur in the clipped week are included
    in 'order' so you can print without fake 'Mon 0' on partial weeks.
    """
    start_d = start_utc.astimezone(PT).date()
    end_d   = (end_utc - timedelta(seconds=1)).astimezone(PT).date()

    weeks = []
    cur = _start_of_week(start_d)
    while cur <= end_d:
        week_start = max(start_d, cur)
        week_end   = min(end_d, cur + timedelta(days=6))

        # Query per-date rows for this week slice
        s_pt  = datetime.combine(week_start, datetime.min.time(), PT)
        e_pt  = datetime.combine(week_end + timedelta(days=1), datetime.min.time(), PT)  # exclusive
        s_utc = s_pt.astimezone(UTC).replace(tzinfo=None)
        e_utc = e_pt.astimezone(UTC).replace(tzinfo=None)
        rows  = _breakdown_by_date(conn, manager_id, s_utc, e_utc, user_id=user_id, extension=extension)

        # Aggregate by weekday
        agg = {d: {"inbound": 0, "outbound": 0, "total": 0} for d in ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]}
        totals = {"inbound": 0, "outbound": 0, "total": 0}

        for r in rows:
            d = r["local_date"] if isinstance(r["local_date"], date) else datetime.strptime(r["local_date"], "%Y-%m-%d").date()
            key = DOW_NAMES[d.weekday()][:3]
            ib  = int(r["inbound_calls"] or 0)
            ob  = int(r["outbound_calls"] or 0)
            tt  = ib + ob
            agg[key]["inbound"]  += ib
            agg[key]["outbound"] += ob
            agg[key]["total"]    += tt
            totals["inbound"]    += ib
            totals["outbound"]   += ob
            totals["total"]      += tt

        # Only include weekdays actually present in the slice
        present_order = [d for d in ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"] if sum(agg[d].values()) > 0]

        weeks.append({
            "start": str(week_start),
            "end": str(week_end),
            "month_label": f"{week_start.year}-{week_start.month:02d}",
            "by_day_all": agg,
            "totals": totals,
            "order": present_order,
        })

        cur += timedelta(days=7)

    return weeks

# ---------- Routes ----------
@ai_bp.route("/api/ai/business-metrics", methods=["GET"])
@login_required
def get_business_metrics():
    try:
        user_id = session.get("user_id")
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT metric_name, value, month, year
            FROM business_metrics
            WHERE user_id = %s
            ORDER BY year, month
        """, (user_id,))
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(data)
    except Exception as e:
        print("❌ Error fetching business metrics:", str(e))
        return jsonify({"error": str(e)}), 500

@ai_bp.route("/api/ai_ask", methods=["POST"])
@login_required
def ai_ask():
    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        data = request.get_json()
        question = data.get("question") or ""
        dashboard_data = data.get("dashboard_data")
        business_metrics = data.get("business_metrics")

        if dashboard_data is None:
            return jsonify({"error": "Missing input"}), 400

        manager_id = session.get("user_id")

        # Detect time range and intents
        start_utc, end_utc, range_label = _parse_range_natural(question)
        wants_calls = _wants_calls(question)
        wants_talk  = _wants_talk_time(question)
        wants_in    = _wants_inbound(question)
        wants_out   = _wants_outbound(question)
        wants_comp  = _wants_compare_previous(question)
        wants_weekly = _wants_weekly_dow_totals(question)
        ext = _extract_extension(question)
        user_id = None  # Optional: resolve names -> user_id

        computed_range = None
        prev_computed  = None
        prev_label     = None

        if start_utc and end_utc and (wants_calls or wants_talk or wants_in or wants_out):
            conn = get_db_connection()

            # Current period
            computed_range = _query_calls_range(
                conn, manager_id, start_utc, end_utc,
                direction=None, user_id=user_id, extension=ext
            )

            # Previous period if asked to compare
            if wants_comp:
                p_start, p_end = _prev_period(start_utc, end_utc)
                prev_computed = _query_calls_range(
                    conn, manager_id, p_start, p_end,
                    direction=None, user_id=user_id, extension=ext
                )
                prev_label = _label_dates_pt(p_start, p_end)

            conn.close()

        # ---------- Direct previous-period comparison ----------
        if computed_range and prev_computed:
            current_label = _label_dates_pt(start_utc, end_utc)

            def line(metric_name: str, curr: int, prev: int) -> str:
                delta = curr - prev
                pct = _pct_change(curr, prev)
                return f"{metric_name}: {curr} vs {prev} ({delta:+d}, {pct})."

            pieces = []
            if wants_in and not wants_out and not wants_calls:
                pieces.append(line("Inbound", computed_range["inbound_calls"], prev_computed["inbound_calls"]))
            elif wants_out and not wants_in and not wants_calls:
                pieces.append(line("Outbound", computed_range["outbound_calls"], prev_computed["outbound_calls"]))
            else:
                pieces.append(line("Inbound",  computed_range["inbound_calls"],  prev_computed["inbound_calls"]))
                pieces.append(line("Outbound", computed_range["outbound_calls"], prev_computed["outbound_calls"]))

            resp_text = f"Last period ({current_label}) vs prior ({prev_label}) — " + " ".join(pieces)

            return jsonify({
                "response": resp_text,
                "range_label": range_label,
                "computed": computed_range,
                "previous": {"label": prev_label, "values": prev_computed}
            })

        # ---------- Weekly × DOW table (explicit weekly requests only) ----------
        if start_utc and end_utc and wants_weekly and (wants_calls or wants_in or wants_out):
            conn = get_db_connection()
            weekly = _weekly_dow_totals(conn, manager_id, start_utc, end_utc,
                                        user_id=user_id, extension=ext)
            conn.close()

            metric = "outbound" if (wants_out and not wants_in) else ("inbound" if (wants_in and not wants_out) else "total")
            lines = []
            for w in weekly:
                parts = [f"{d} {w['by_day_all'][d][metric]}" for d in w["order"]]
                lines.append(f"{w['start']}–{w['end']}: " + ", ".join(parts) + f" (total {w['totals'][metric]}).")

            current_label = _label_dates_pt(start_utc, end_utc)
            return jsonify({
                "response": f"Day-of-week {metric} totals by week for {current_label}:\n" + "\n".join(lines),
                "range_label": current_label,
                "weekly_by_dow": weekly
            })

        # ---------- Day-of-week winner (single best weekday over the whole range) ----------
        if start_utc and end_utc and _wants_day_of_week(question) and (wants_calls or wants_in or wants_out):
            conn = get_db_connection()
            daily = _breakdown_by_date(conn, manager_id, start_utc, end_utc,
                                       user_id=user_id, extension=ext)
            conn.close()

            inbound_by_dow  = [0]*7
            outbound_by_dow = [0]*7
            total_by_dow    = [0]*7

            def _to_date(x):
                if isinstance(x, date): return x
                return datetime.strptime(x, "%Y-%m-%d").date()

            per_date = []
            for r in daily:
                d = _to_date(r["local_date"]); wd = d.weekday()
                ib = int(r["inbound_calls"] or 0); ob = int(r["outbound_calls"] or 0)
                inbound_by_dow[wd] += ib; outbound_by_dow[wd] += ob; total_by_dow[wd] += (ib+ob)
                per_date.append({"date": d, "inbound": ib, "outbound": ob, "total": ib+ob})

            if wants_out and not wants_in:
                series, metric_name = outbound_by_dow, "outbound"
            elif wants_in and not wants_out:
                series, metric_name = inbound_by_dow, "inbound"
            else:
                series, metric_name = total_by_dow, "total"

            best_wd = max(range(7), key=lambda i: series[i])
            best_name, best_total = DOW_NAMES[best_wd], series[best_wd]

            span_days = (end_utc - start_utc).days
            best_date_text = ""
            if span_days <= 7:
                key = "outbound" if (wants_out and not wants_in) else ("inbound" if (wants_in and not wants_out) else "total")
                peak_row = max(per_date, key=lambda r: r[key])
                best_date_text = f" Peak date: {peak_row['date']} ({peak_row[key]})."

            current_label = _label_dates_pt(start_utc, end_utc)
            resp = f"{best_name} had the most {metric_name} calls in {current_label} with {best_total}.{best_date_text}"

            return jsonify({"response": resp, "range_label": current_label, "daily": daily})

        # ---------- LLM fallback ----------
        extra_block = ""
        if computed_range:
            extra_block = f"""
Computed time-range results ({range_label}):
- Inbound Calls: {computed_range['inbound_calls']}
- Outbound Calls: {computed_range['outbound_calls']}
- Total Calls: {computed_range['total_calls']}
- Inbound Talk Time: {_fmt_hms(computed_range['inbound_talk_seconds'])}
- Outbound Talk Time: {_fmt_hms(computed_range['outbound_talk_seconds'])}
- Handle Time: {_fmt_hms(computed_range['handle_seconds'])}
"""

        prompt = f"""
You are Reflexx AI, an assistant helping a manager understand performance data for an insurance agency.
Always answer succinctly with the exact numbers, the time range, and how you calculated them. If a question implies a time range, prefer the computed results below.

Here is today's dashboard data:
{dashboard_data}

Here is historical business metric data:
{business_metrics}
{extra_block}

Manager's question: "{question}"
"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )

        return jsonify({
            "response": response.choices[0].message.content,
            "range_label": range_label,
            "computed": computed_range
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Optional programmatic endpoint for widgets/charts ----------
@ai_bp.route("/api/ai/call-stats", methods=["GET"])
@login_required
def call_stats():
    """
    Example: /api/ai/call-stats?range=last week&extension=15689111
    Returns totals for inbound/outbound calls and talk times over the given range.
    """
    try:
        manager_id = session.get("user_id")
        q = request.args.get("range", "")
        ext = request.args.get("extension")
        user_id = request.args.get("user_id")  # optional

        start_utc, end_utc, label = _parse_range_natural(q)
        if not (start_utc and end_utc):
            return jsonify({"error": "Unrecognized range"}), 400

        conn = get_db_connection()
        row = _query_calls_range(conn, manager_id, start_utc, end_utc, user_id=user_id, extension=ext)
        conn.close()

        return jsonify({
            "range_label": label,
            "inbound_calls": row["inbound_calls"],
            "outbound_calls": row["outbound_calls"],
            "total_calls": row["total_calls"],
            "inbound_talk_seconds": row["inbound_talk_seconds"],
            "outbound_talk_seconds": row["outbound_talk_seconds"],
            "handle_seconds": row["handle_seconds"],
            "inbound_talk_pretty": _fmt_hms(row["inbound_talk_seconds"]),
            "outbound_talk_pretty": _fmt_hms(row["outbound_talk_seconds"]),
            "handle_pretty": _fmt_hms(row["handle_seconds"]),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
