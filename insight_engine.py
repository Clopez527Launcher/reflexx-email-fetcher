# insight_engine.py
import os
import json
import argparse
import mysql.connector
from openai import OpenAI
from datetime import timedelta, date
from datetime import datetime

def to_float(x):
    """Safely convert Decimal/None/int/float to float."""
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


from insight_config import (
    TARGET_STRENGTH_CANDIDATES,
    TARGET_WEAKNESS_CANDIDATES,
    FINAL_STRENGTHS_TO_SHOW,
    FINAL_WEAKNESSES_TO_SHOW,
    ALLOW_FEWER_IF_NOT_ENOUGH,
    MAX_CANDIDATES_PER_MANAGER,
    MIN_PREV_FOR_DELTA,
    MAX_ABS_DP_FOR_SEVERITY,
)

# ---------------------------
# CONFIG: MySQL connection
# ---------------------------
# ✅ MySQL Configuration (Railway INTERNAL)
# (Works only inside Railway services)
DB_HOST = "mysql.railway.internal"
DB_USER = "root"
DB_PASSWORD = "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc"  # keep your real password here
DB_NAME = "railway"

MYSQL_CONFIG = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": 3306
}

# ---------------------------
# Helper: detect best display-name column in users table
# ---------------------------
def resolve_user_name_sql(conn):
    """
    Finds the best 'display name' column in users table.
    Priority order: name, full_name, display_name, username, nickname, email.
    Returns a SQL snippet like "u.full_name".
    """
    preferred = ["name", "full_name", "display_name", "username", "nickname", "email"]

    cur = conn.cursor()
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'users'
          AND column_name IN (%s,%s,%s,%s,%s,%s)
    """, preferred)

    found = {row[0] for row in cur.fetchall()}

    for col in preferred:
        if col in found:
            return f"u.{col}"

    # last-resort fallback so script still runs
    return "CAST(u.id AS CHAR)"

# ---------------------------
# Helper: date windows
# ---------------------------
def build_windows():
    """
    Windows you want:
      - last 7 days vs previous 7 days
      - last 14 days vs previous 14 days
      - last 30 days vs previous 30 days

    IMPORTANT:
    We anchor to YESTERDAY so we don't include incomplete "today" data.
    """
    anchor = date.today() - timedelta(days=1)

    def win(n):
        start_dt = anchor - timedelta(days=n-1)
        end_dt = anchor
        return start_dt, end_dt, f"last_{n}_days"

    return [
        win(7),
        win(14),
        win(30),
    ]

# ---------------------------
# Candidate generation SQLs
# Uses fact_daily + users(manager_id)
# ---------------------------

def q_metric_delta(conn, mgr_id, metric_col, start_dt, end_dt, USER_NAME_SQL, min_prev=MIN_PREV_FOR_DELTA):
    """
    Compare current N-day window vs previous N-day window for each user.
    Returns rows:
      user_id, name, cur_val, prev_val, delta_pct

    min_prev lets each rule require a bigger baseline before producing a delta.
    """
    days = (end_dt - start_dt).days + 1
    prev_end = start_dt - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days-1)

    sql = f"""
    WITH cur AS (
      SELECT fd.user_id,
             SUM(fd.{metric_col}) AS cur_val
      FROM fact_daily fd
      JOIN users u ON u.id = fd.user_id
      WHERE u.manager_id = %s
        AND fd.date BETWEEN %s AND %s
      GROUP BY fd.user_id
    ),
    prev AS (
      SELECT fd.user_id,
             SUM(fd.{metric_col}) AS prev_val
      FROM fact_daily fd
      JOIN users u ON u.id = fd.user_id
      WHERE u.manager_id = %s
        AND fd.date BETWEEN %s AND %s
      GROUP BY fd.user_id
    )
    SELECT u.id AS user_id,
           {USER_NAME_SQL} AS name,
           COALESCE(cur.cur_val,0) AS cur_val,
           COALESCE(prev.prev_val,0) AS prev_val,
           CASE
             WHEN COALESCE(prev.prev_val,0) < %s THEN NULL
             ELSE ((COALESCE(cur.cur_val,0) - prev.prev_val) / prev.prev_val) + 0.0
           END AS delta_pct
    FROM users u
    LEFT JOIN cur  ON cur.user_id = u.id
    LEFT JOIN prev ON prev.user_id = u.id
    WHERE u.manager_id = %s
    """

    cur = conn.cursor(dictionary=True)
    cur.execute(sql, (
        mgr_id, start_dt, end_dt,
        mgr_id, prev_start, prev_end,
        min_prev,   # ✅ per-rule minimum baseline
        mgr_id
    ))
    return cur.fetchall()


def q_idle_avg(conn, mgr_id, start_dt, end_dt, USER_NAME_SQL):
    sql = f"""
    SELECT u.id AS user_id,
           {USER_NAME_SQL} AS name,
           AVG(fd.idle_time_seconds) + 0.0 AS idle_avg_sec
    FROM fact_daily fd
    JOIN users u ON u.id = fd.user_id
    WHERE u.manager_id = %s
      AND fd.date BETWEEN %s AND %s
    GROUP BY u.id, name
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, (mgr_id, start_dt, end_dt))
    return cur.fetchall()

def q_top_rank(conn, mgr_id, metric_col, start_dt, end_dt, USER_NAME_SQL, top_n=1):
    sql = f"""
    SELECT u.id AS user_id,
           {USER_NAME_SQL} AS name,
           SUM(fd.{metric_col}) + 0.0 AS total_val
    FROM fact_daily fd
    JOIN users u ON u.id = fd.user_id
    WHERE u.manager_id = %s
      AND fd.date BETWEEN %s AND %s
    GROUP BY u.id, name
    ORDER BY total_val DESC
    LIMIT {top_n}
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, (mgr_id, start_dt, end_dt))
    return cur.fetchall()

# ---------------------------
# Insight templates (v1)
# phone / quoting / movement only
# ---------------------------

def q_active_days_pair(conn, mgr_id, start_dt, end_dt, USER_NAME_SQL):
    """
    For each user, count active days in:
      - current window
      - previous window (same length)
    Active day = any meaningful activity > 0 in fact_daily.

    Returns rows:
      user_id, name, cur_active_days, prev_active_days
    """
    days = (end_dt - start_dt).days + 1
    prev_end = start_dt - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days-1)

    # Define what counts as "active"
    # tweak list anytime, but keep it broad so PTO/absence shows as inactive
    active_expr = """
      (COALESCE(fd.outbounds,0)
     + COALESCE(fd.inbounds,0)
     + COALESCE(fd.quotes_unique,0)
     + COALESCE(fd.keystrokes,0)
     + COALESCE(fd.mouse_clicks,0)
     + COALESCE(fd.advisor_pro_minutes,0)
     + COALESCE(fd.ob_time_minutes,0)
     + COALESCE(fd.ib_time_minutes,0)
     + COALESCE(fd.vc_items,0)) > 0
    """

    sql = f"""
    WITH cur AS (
      SELECT fd.user_id,
             SUM(CASE WHEN {active_expr} THEN 1 ELSE 0 END) AS cur_active_days
      FROM fact_daily fd
      JOIN users u ON u.id = fd.user_id
      WHERE u.manager_id = %s
        AND fd.date BETWEEN %s AND %s
      GROUP BY fd.user_id
    ),
    prev AS (
      SELECT fd.user_id,
             SUM(CASE WHEN {active_expr} THEN 1 ELSE 0 END) AS prev_active_days
      FROM fact_daily fd
      JOIN users u ON u.id = fd.user_id
      WHERE u.manager_id = %s
        AND fd.date BETWEEN %s AND %s
      GROUP BY fd.user_id
    )
    SELECT u.id AS user_id,
           {USER_NAME_SQL} AS name,
           COALESCE(cur.cur_active_days,0) AS cur_active_days,
           COALESCE(prev.prev_active_days,0) AS prev_active_days
    FROM users u
    LEFT JOIN cur  ON cur.user_id = u.id
    LEFT JOIN prev ON prev.user_id = u.id
    WHERE u.manager_id = %s
    """

    cur2 = conn.cursor(dictionary=True)
    cur2.execute(sql, (
        mgr_id, start_dt, end_dt,
        mgr_id, prev_start, prev_end,
        mgr_id
    ))
    return cur2.fetchall()

def gen_candidates(conn, mgr_id, start_dt, end_dt, label, USER_NAME_SQL):
    candidates = []
    
    # Build active-days lookup for PTO / absence filtering
    active_rows = q_active_days_pair(conn, mgr_id, start_dt, end_dt, USER_NAME_SQL)
    active_map = {r["user_id"]: r for r in active_rows}

    window_days = (end_dt - start_dt).days + 1
    # Require at least 50% of days active in previous window (min 3 days)
    min_prev_active_days = max(3, int(window_days * 0.5))


    def add_candidate(row, insight_type, polarity, title, message, metrics, severity):
        candidates.append({
            "manager_id": mgr_id,
            "user_id": row.get("user_id"),
            "insight_type": insight_type,
            "polarity": polarity,
            "raw_title": title,
            "raw_message": message,
            "metric_json": metrics,
            "severity_score": float(severity),
            "start_date": start_dt,
            "end_date": end_dt,
            "window_label": label
        })

    # 1) Outbounds delta up / down
    for r in q_metric_delta(conn, mgr_id, "outbounds", start_dt, end_dt, USER_NAME_SQL, min_prev=50):
        dp = to_float(r.get("delta_pct"))
        if dp is None:
            continue
            
        # PTO/absence filter using "null days" in PREVIOUS window
        # If too many days were null last window, skip % delta insights.
        prev_window_days = (end_dt - start_dt).days + 1

        max_null_by_window = {
            "last_7_days": 3,   # skip if >3 null days in prev 7
            "last_14_days": 6,  # skip if >6 null days in prev 14
            "last_30_days": 12, # skip if >12 null days in prev 30
        }
        max_nulls = max_null_by_window.get(label, 3)

        ar = active_map.get(r["user_id"], {})
        prev_active = int(ar.get("prev_active_days", 0) or 0)

        # null days = total days - active days
        prev_nulls = max(prev_window_days - prev_active, 0)

        if prev_nulls > max_nulls:
            continue

        dp_for_sev = max(-MAX_ABS_DP_FOR_SEVERITY, min(MAX_ABS_DP_FOR_SEVERITY, dp))
        severity = abs(dp_for_sev) * 2.0  # keep v1 weight

        if dp >= 0.15:
            add_candidate(
                r, "outbounds_up", "strength",
                "Outbound calls up",
                f"{r['name']} outbound calls are up {dp:.0%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )
        elif dp <= -0.15:
            add_candidate(
                r, "outbounds_down", "weakness",
                "Outbound calls down",
                f"{r['name']} outbound calls are down {abs(dp):.0%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )

    # 2) Inbounds delta up / down
    for r in q_metric_delta(conn, mgr_id, "inbounds", start_dt, end_dt, USER_NAME_SQL, min_prev=20):
        dp = to_float(r.get("delta_pct"))
        if dp is None:
            continue

        dp_for_sev = max(-MAX_ABS_DP_FOR_SEVERITY, min(MAX_ABS_DP_FOR_SEVERITY, dp))
        severity = abs(dp_for_sev) * 1.8  # keep v1 weight

        if dp >= 0.15:
            add_candidate(
                r, "inbounds_up", "strength",
                "Inbound calls up",
                f"{r['name']} inbound calls are up {dp:.0%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )
        elif dp <= -0.15:
            add_candidate(
                r, "inbounds_down", "weakness",
                "Inbound calls down",
                f"{r['name']} inbound calls are down {abs(dp):.0%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )

    # 3) Quotes unique delta up / down
    for r in q_metric_delta(conn, mgr_id, "quotes_unique", start_dt, end_dt, USER_NAME_SQL, min_prev=10):
        dp = to_float(r.get("delta_pct"))
        if dp is None:
            continue

        dp_for_sev = max(-MAX_ABS_DP_FOR_SEVERITY, min(MAX_ABS_DP_FOR_SEVERITY, dp))
        severity = abs(dp_for_sev) * 2.2  # keep v1 weight

        if dp >= 0.12:
            add_candidate(
                r, "quotes_unique_up", "strength",
                "Quotes up",
                f"{r['name']} unique quotes are up {dp:.0%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )
        elif dp <= -0.12:
            add_candidate(
                r, "quotes_unique_down", "weakness",
                "Quotes down",
                f"{r['name']} unique quotes are down {abs(dp):.0%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )

    # 4) Very low quotes absolute (only for >=5 day windows)
    window_days = (end_dt - start_dt).days + 1
    if window_days >= 5:
        sql_low_quotes = f"""
        SELECT u.id AS user_id,
               {USER_NAME_SQL} AS name,
               SUM(fd.quotes_unique) AS quotes_total
        FROM fact_daily fd
        JOIN users u ON u.id = fd.user_id
        WHERE u.manager_id = %s
          AND fd.date BETWEEN %s AND %s
        GROUP BY u.id, name
        HAVING quotes_total < 15
        """
        cur = conn.cursor(dictionary=True)
        cur.execute(sql_low_quotes, (mgr_id, start_dt, end_dt))
        for r in cur.fetchall():
            severity = 1.7
            add_candidate(
                r, "quotes_low_abs", "weakness",
                "Low Quotes",
                f"{r['name']} only has {r['quotes_total']} unique quotes ({label}).",
                {"quotes_total": float(r["quotes_total"] or 0)},
                severity
            )

    # 5) Idle time high
    for r in q_idle_avg(conn, mgr_id, start_dt, end_dt, USER_NAME_SQL):
        idle_avg_sec = to_float(r.get("idle_avg_sec")) or 0.0
        idle_hours = idle_avg_sec / 3600.0

        if idle_hours >= 1.5:
            # 1.5h -> sev 1.0, 3.0h -> sev 2.0, etc.
            severity = idle_hours / 1.5

            add_candidate(
                r, "idle_high", "weakness",
                "High Idle Time",
                f"{r['name']} averaged {idle_hours:.1f} idle hrs/day ({label}).",
                {"idle_avg_hours": idle_hours, "idle_avg_sec": idle_avg_sec},
                severity
            )

    # 6) Idle time improved (down >=20%)
    for r in q_metric_delta(conn, mgr_id, "idle_time_seconds", start_dt, end_dt, USER_NAME_SQL):
        dp = to_float(r.get("delta_pct"))
        if dp is None:
            continue

        if dp <= -0.20:
            dp_for_sev = max(-MAX_ABS_DP_FOR_SEVERITY, min(MAX_ABS_DP_FOR_SEVERITY, dp))
            severity = abs(dp_for_sev) * 1.4  # keep v1 weight

            add_candidate(
                r, "idle_improved", "strength",
                "Idle Time Improved",
                f"{r['name']} idle time improved {abs(dp):.0%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )

    # 7) Top outbound caller (team-level strength)
    top_ob = q_top_rank(conn, mgr_id, "outbounds", start_dt, end_dt, USER_NAME_SQL, top_n=1)
    for r in top_ob:
        severity = 1.1
        add_candidate(
            r, "top_outbounds", "strength",
            "Top Outbound Producer",
            f"{r['name']} led outbounds with {int(r['total_val'])} calls ({label}).",
            {"outbounds_total": float(r["total_val"] or 0)},
            severity
        )

    # 8) Top quoter (team-level strength)
    top_q = q_top_rank(conn, mgr_id, "quotes_unique", start_dt, end_dt, USER_NAME_SQL, top_n=1)
    for r in top_q:
        severity = 1.1
        add_candidate(
            r, "top_quoter", "strength",
            "Top Quoter",
            f"{r['name']} led quoting with {int(r['total_val'])} unique quotes ({label}).",
            {"quotes_unique_total": float(r["total_val"] or 0)},
            severity
        )

    # 9) Outbound talk time delta up / down (ob_time_minutes)
    for r in q_metric_delta(conn, mgr_id, "ob_time_minutes", start_dt, end_dt, USER_NAME_SQL, min_prev=30):
        dp = to_float(r.get("delta_pct"))
        if dp is None:
            continue

        dp_for_sev = max(-MAX_ABS_DP_FOR_SEVERITY, min(MAX_ABS_DP_FOR_SEVERITY, dp))
        severity = abs(dp_for_sev) * 2.0  # v1 weight

        if dp >= 0.10:
            add_candidate(
                r, "ob_time_up", "strength",
                "Outbound talk time up",
                f"{r['name']} outbound talk time is up {dp:.1%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )
        elif dp <= -0.10:
            add_candidate(
                r, "ob_time_down", "weakness",
                "Outbound talk time down",
                f"{r['name']} outbound talk time is down {abs(dp):.1%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )


    # Dynamic baseline for AdvisorPro based on window length
    window_days = (end_dt - start_dt).days + 1
    advisor_min_prev_by_window = {
        7:  90,   # 1.5 hours
        14: 180,  # 3 hours
        30: 360,  # 6 hours
    }
    advisor_min_prev = advisor_min_prev_by_window.get(window_days, 90)

    # 10) AdvisorPro minutes delta up / down
    for r in q_metric_delta(conn, mgr_id, "advisor_pro_minutes", start_dt, end_dt, USER_NAME_SQL, min_prev=advisor_min_prev):
        dp = to_float(r.get("delta_pct"))
        if dp is None:
            continue

        dp_for_sev = max(-MAX_ABS_DP_FOR_SEVERITY, min(MAX_ABS_DP_FOR_SEVERITY, dp))
        severity = abs(dp_for_sev) * 1.6  # v1 weight

        if dp >= 0.10:
            add_candidate(
                r, "advisor_pro_up", "strength",
                "AdvisorPro usage up",
                f"{r['name']} AdvisorPro time is up {dp:.1%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )
        elif dp <= -0.10:
            add_candidate(
                r, "advisor_pro_down", "weakness",
                "AdvisorPro usage down",
                f"{r['name']} AdvisorPro time is down {abs(dp):.1%} ({label}).",
                {"cur": float(r["cur_val"] or 0), "prev": float(r["prev_val"] or 0), "delta_pct": dp},
                severity
            )

            
    return candidates
    

# ---------------------------
# Trim to top 30/30 for AI judge
# ---------------------------
def trim_candidates(all_candidates):
    strengths = [c for c in all_candidates if c["polarity"] == "strength"]
    weaknesses = [c for c in all_candidates if c["polarity"] == "weakness"]

    strengths.sort(key=lambda x: x["severity_score"], reverse=True)
    weaknesses.sort(key=lambda x: x["severity_score"], reverse=True)

    top_strengths = strengths[:TARGET_STRENGTH_CANDIDATES]
    top_weaknesses = weaknesses[:TARGET_WEAKNESS_CANDIDATES]

    trimmed = top_strengths + top_weaknesses
    trimmed.sort(key=lambda x: x["severity_score"], reverse=True)

    return trimmed[:MAX_CANDIDATES_PER_MANAGER], top_strengths, top_weaknesses


#Gpt v2 Judge
def is_top_metric(c):
    return str(c.get("insight_type", "")).startswith("top_")

def polish_after_gpt(items, top_pool, final_k, top_cap=1, max_per_user=1):
    """
    Enforces:
      - <= top_cap top_* insights total (even during refill)
      - spread across users up to max_per_user if possible
      - refill by severity with rules respected
    """

    def sev(c): 
        return c.get("severity_score") or 0

    # Sort incoming items by severity (keep GPT order isn't critical post-polish)
    items = sorted(items, key=sev, reverse=True)

    picked = []
    user_counts = {}
    top_count = 0

    def can_pick(c):
        nonlocal top_count
        if is_top_metric(c) and top_count >= top_cap:
            return False
        uid = c["user_id"]
        if user_counts.get(uid, 0) >= max_per_user:
            return False
        return True

    def do_pick(c):
        nonlocal top_count
        picked.append(c)
        uid = c["user_id"]
        user_counts[uid] = user_counts.get(uid, 0) + 1
        if is_top_metric(c):
            top_count += 1

    # 1) take from current items first
    for c in items:
        if c in picked:
            continue
        if can_pick(c):
            do_pick(c)
        if len(picked) == final_k:
            return picked

    # 2) refill from pool, respecting caps
    pool = sorted([c for c in top_pool if c not in picked], key=sev, reverse=True)
    for c in pool:
        if can_pick(c):
            do_pick(c)
        if len(picked) == final_k:
            break

    # 3) absolute last resort: ignore user spread, still respect top_cap
    if len(picked) < final_k:
        for c in pool:
            if c in picked:
                continue
            if is_top_metric(c) and top_count >= top_cap:
                continue
            picked.append(c)
            if len(picked) == final_k:
                break

    return picked[:final_k]

def gpt_select_top(top_strengths, top_weaknesses, window_label, all_candidates=None):
    """
    GPT judge: pick top 3 strengths + top 3 weaknesses.
    Falls back to severity if anything fails.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        strengths = sorted(top_strengths, key=lambda x: x["severity_score"], reverse=True)
        weaknesses = sorted(top_weaknesses, key=lambda x: x["severity_score"], reverse=True)
        return strengths[:FINAL_STRENGTHS_TO_SHOW], weaknesses[:FINAL_WEAKNESSES_TO_SHOW]

    client = OpenAI(api_key=api_key)

    def make_cid(c, kind):
        # kind is "strength" or "weakness"
        return f"{kind}|{c['user_id']}|{c['insight_type']}|{c['window_label']}"

    strengths_payload = [
        {
            "cid": make_cid(c, "strength"),
            "user_id": c["user_id"],
            "insight_type": c["insight_type"],
            "title": c["raw_title"],
            "message": c["raw_message"],
            "severity": c["severity_score"],
            "window": c["window_label"],
        }
        for c in top_strengths
    ]

    weaknesses_payload = [
        {
            "cid": make_cid(c, "weakness"),
            "user_id": c["user_id"],
            "insight_type": c["insight_type"],
            "title": c["raw_title"],
            "message": c["raw_message"],
            "severity": c["severity_score"],
            "window": c["window_label"],
        }
        for c in top_weaknesses
    ]

    system_prompt = f"""
You are selecting the best performance insights for a manager dashboard.

Pick EXACTLY:
- 3 strengths
- 3 weaknesses
for this window: {window_label}

Rules:
- Avoid redundancy (don’t pick two that say the same thing).
- Prefer meaningful/behavioral insights over generic “top_*” unless top_* is clearly best.
- Severity matters, but use common sense.
- If something looks like a return-from-PTO artifact, skip it.

Return STRICT JSON ONLY:

{{
  "strength_cids": ["cid1","cid2","cid3"],
  "weakness_cids": ["cidA","cidB","cidC"]
}}
""".strip()

    user_prompt = json.dumps({
        "strength_candidates": strengths_payload,
        "weakness_candidates": weaknesses_payload
    })

    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=400,
            response_format={"type": "json_object"},  # ✅ supported here
        )

        content = (res.choices[0].message.content or "").strip()

        if not content:
            raise ValueError("GPT returned empty message.content")

        data = json.loads(content)


        # GPT chooses these, we will validate + keep order
        strength_cids = [cid for cid in data.get("strength_cids", []) if str(cid).startswith("strength|")]
        weakness_cids = [cid for cid in data.get("weakness_cids", []) if str(cid).startswith("weakness|")]

        # map by cid for fast lookup
        strength_by_cid = {make_cid(c, "strength"): c for c in top_strengths}
        weakness_by_cid = {make_cid(c, "weakness"): c for c in top_weaknesses}

        # preserve GPT order, ignore unknown cids
        final_strengths = [strength_by_cid[cid] for cid in strength_cids if cid in strength_by_cid]
        final_weaknesses = [weakness_by_cid[cid] for cid in weakness_cids if cid in weakness_by_cid]


        # Fallback if GPT returns too few
        if len(final_strengths) < FINAL_STRENGTHS_TO_SHOW:
            pool = [c for c in top_strengths if c not in final_strengths]
            pool = sorted(pool, key=lambda x: x["severity_score"], reverse=True)
            final_strengths += pool[:(FINAL_STRENGTHS_TO_SHOW - len(final_strengths))]

        if len(final_weaknesses) < FINAL_WEAKNESSES_TO_SHOW:
            pool = [c for c in top_weaknesses if c not in final_weaknesses]
            pool = sorted(pool, key=lambda x: x["severity_score"], reverse=True)
            final_weaknesses += pool[:(FINAL_WEAKNESSES_TO_SHOW - len(final_weaknesses))]
            
        # ----- weakness rescue if trimmed pool is too weak -----
        if all_candidates:
            best_weak_sev = max((c["severity_score"] for c in final_weaknesses), default=0)
            if best_weak_sev < 1.0:
                full_weak = [c for c in all_candidates if c["polarity"] == "weakness"]
                full_weak.sort(key=lambda x: x["severity_score"], reverse=True)
                final_weaknesses = full_weak[:FINAL_WEAKNESSES_TO_SHOW]
            

        # ----- deterministic polish pass -----
        final_strengths = polish_after_gpt(
            final_strengths, top_strengths, FINAL_STRENGTHS_TO_SHOW,
            top_cap=1,          # <= 1 top_* strength
            max_per_user=1      # try to spread users
        )

        final_weaknesses = polish_after_gpt(
            final_weaknesses, top_weaknesses, FINAL_WEAKNESSES_TO_SHOW,
            top_cap=1,          # optional but consistent
            max_per_user=1
        )

        return final_strengths, final_weaknesses


    except Exception as e:
        print("[GPT JUDGE ERROR]", e)
        try:
            print("[GPT RAW OUTPUT]", content)
        except Exception:
            pass

        strengths = sorted(top_strengths, key=lambda x: x["severity_score"], reverse=True)
        weaknesses = sorted(top_weaknesses, key=lambda x: x["severity_score"], reverse=True)
        return strengths[:FINAL_STRENGTHS_TO_SHOW], weaknesses[:FINAL_WEAKNESSES_TO_SHOW]

# ---------------------------
# v1 "judge" (no Mistral yet)
# Just take highest severity
# ---------------------------
def v1_select_top(trimmed):
    strengths = [c for c in trimmed if c["polarity"] == "strength"]
    weaknesses = [c for c in trimmed if c["polarity"] == "weakness"]
    strengths.sort(key=lambda x: x["severity_score"], reverse=True)
    weaknesses.sort(key=lambda x: x["severity_score"], reverse=True)
    return strengths[:FINAL_STRENGTHS_TO_SHOW], weaknesses[:FINAL_WEAKNESSES_TO_SHOW]

# ---------------------------
# MAIN RUNNER
# ---------------------------
def save_ai_insights(conn, mgr_id, window_label, start_dt, end_dt, strengths, weaknesses):
    cur = conn.cursor()

    # overwrite current dashboard picks for that window
    if window_label == "overall":
        cur.execute("""
            DELETE FROM ai_dashboard_insights
            WHERE manager_id=%s AND window_label=%s
        """, (mgr_id, window_label))
    else:
        cur.execute("""
            DELETE FROM ai_dashboard_insights
            WHERE manager_id=%s AND window_label=%s AND start_date=%s AND end_date=%s
        """, (mgr_id, window_label, start_dt, end_dt))


    rows = []
    for c in strengths:
        # If this row came from a real window (last_7_days / last_14_days / last_30_days),
        # use that label for the suffix. Otherwise fall back to the passed-in window_label.
        label_for_pretty = c.get("window_label") or window_label

        rows.append((
            mgr_id, window_label, "strength",
            c["user_id"], c["insight_type"],
            c.get("raw_title"), prettify_message(c.get("raw_message"), label_for_pretty),
            c.get("severity_score", 0),
            start_dt, end_dt
        ))


    for c in weaknesses:
        # Same idea for weaknesses
        label_for_pretty = c.get("window_label") or window_label

        rows.append((
            mgr_id, window_label, "weakness",
            c["user_id"], c["insight_type"],
            c.get("raw_title"), prettify_message(c.get("raw_message"), label_for_pretty),
            c.get("severity_score", 0),
            start_dt, end_dt
        ))

    cur.executemany("""
        INSERT INTO ai_dashboard_insights
        (manager_id, window_label, polarity, user_id, insight_type, raw_title, raw_message, severity_score, start_date, end_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, rows)

    conn.commit()
    cur.close()

#FINAL TOP 3 Selection
def gpt_select_overall(final_strengths_by_window, final_weaknesses_by_window):
    """
    Take the 18 window winners (9 strengths + 9 weaknesses),
    and pick overall top 3 + top 3.
    """

    all_strengths = []
    all_weaknesses = []

    for label, strengths in final_strengths_by_window.items():
        all_strengths.extend(strengths)

    for label, weaknesses in final_weaknesses_by_window.items():
        all_weaknesses.extend(weaknesses)

    # Reuse existing GPT judge but with a fake window label "overall"
    return gpt_select_top(all_strengths, all_weaknesses, "overall")

def refill_if_needed(items, pool, k):
    pool = sorted(pool, key=lambda x: x.get("severity_score") or 0, reverse=True)
    out = items[:]
    for c in pool:
        if c not in out:
            out.append(c)
        if len(out) == k:
            break
    return out[:k]


def run_for_manager(mgr_id):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    
    run_id = f"mgr{mgr_id}_{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}"

    # detect name column once
    USER_NAME_SQL = resolve_user_name_sql(conn)

    final_strengths_by_window = {}
    final_weaknesses_by_window = {}
    window_ranges = {}


    for start_dt, end_dt, label in build_windows():
        all_candidates = gen_candidates(conn, mgr_id, start_dt, end_dt, label, USER_NAME_SQL)
        save_all_candidates(conn, run_id, mgr_id, label, start_dt, end_dt, all_candidates)

        trimmed, top_strengths, top_weaknesses = trim_candidates(all_candidates)

        final_strengths, final_weaknesses = gpt_select_top(
            top_strengths, top_weaknesses, label, all_candidates=all_candidates
        )

        # ✅ hard cap BEFORE saving/storing
        final_strengths = final_strengths[:FINAL_STRENGTHS_TO_SHOW]
        final_weaknesses = final_weaknesses[:FINAL_WEAKNESSES_TO_SHOW]

        final_strengths_by_window[label] = final_strengths
        final_weaknesses_by_window[label] = final_weaknesses
        window_ranges[label] = (start_dt, end_dt)

        save_ai_insights(conn, mgr_id, label, start_dt, end_dt, final_strengths, final_weaknesses)


        print("\n" + "="*70)
        print(f"MANAGER {mgr_id} — {label} ({start_dt} → {end_dt})")
        print(f"Generated candidates: {len(all_candidates)}")
        print(f"Trimmed strengths: {len(top_strengths)} | Trimmed weaknesses: {len(top_weaknesses)}")

        print("\nALL CANDIDATES (that qualified):")
        for i, c in enumerate(all_candidates, 1):
            print(f"  {i}) [{c['polarity']}] [{c['insight_type']}] {c['raw_message']} (sev {c['severity_score']:.2f})")

        print("\nTOP STRENGTHS (v2 GPT pick, fallback=severity):")
        for i, c in enumerate(final_strengths, 1):
            print(f"  {i}) [{c['insight_type']}] {c['raw_message']}  (sev {c['severity_score']:.2f})")

        print("\nTOP WEAKNESSES (v2 GPT pick, fallback=severity):")
        for i, c in enumerate(final_weaknesses, 1):
            print(f"  {i}) [{c['insight_type']}] {c['raw_message']}  (sev {c['severity_score']:.2f})")

    # ---------------------------
    # OVERALL 6-pick for dashboard (run ONCE)
    # ---------------------------

    # Build same pools gpt_select_overall uses (so refill has something real)
    all_strengths = []
    all_weaknesses = []

    for strengths in final_strengths_by_window.values():
        all_strengths.extend(strengths)

    for weaknesses in final_weaknesses_by_window.values():
        all_weaknesses.extend(weaknesses)

    overall_strengths, overall_weaknesses = gpt_select_top(
        all_strengths, all_weaknesses, "overall"
    )

    # Force refill if GPT returns too few
    overall_strengths  = refill_if_needed(overall_strengths,  all_strengths,  FINAL_STRENGTHS_TO_SHOW)
    overall_weaknesses = refill_if_needed(overall_weaknesses, all_weaknesses, FINAL_WEAKNESSES_TO_SHOW)

    # Hard cap
    overall_strengths  = overall_strengths[:FINAL_STRENGTHS_TO_SHOW]
    overall_weaknesses = overall_weaknesses[:FINAL_WEAKNESSES_TO_SHOW]

    overall_start = min(r[0] for r in window_ranges.values())
    overall_end   = max(r[1] for r in window_ranges.values())

    save_ai_insights(conn, mgr_id, "overall", overall_start, overall_end,
                     overall_strengths, overall_weaknesses)

    print("\n" + "="*70)
    print(f"MANAGER {mgr_id} — OVERALL (from {overall_start} → {overall_end})")

    print("\nTOP STRENGTHS (overall):")
    for i, c in enumerate(overall_strengths, 1):
        msg = prettify_message(c.get("raw_message"), c.get("window_label", "overall"))
        print(f"  {i}) [{c['insight_type']}] {msg} (sev {c['severity_score']:.2f})")

    # SAFETY: if somehow weaknesses are empty, refill from full pool by severity
    if not overall_weaknesses:
        overall_weaknesses = sorted(all_weaknesses, key=lambda x: x.get("severity_score") or 0, reverse=True)[:FINAL_WEAKNESSES_TO_SHOW]

    # final cap again just to be safe
    overall_weaknesses = overall_weaknesses[:FINAL_WEAKNESSES_TO_SHOW]


    print("\nTOP WEAKNESSES (overall):")
    for i, c in enumerate(overall_weaknesses, 1):
        msg = prettify_message(c.get("raw_message"), c.get("window_label", "overall"))
        print(f"  {i}) [{c['insight_type']}] {msg} (sev {c['severity_score']:.2f})")



    conn.close()

import re

WINDOW_PRETTY = {
    "last_7_days":  "the last week",
    "last_14_days": "the last couple weeks",
    "last_30_days": "the last month",
    "overall":      "the last month"  # overall spans all windows; month is fine wording
}

def prettify_message(raw_message, window_label):
    """
    Turns:
      "Zach Williams AdvisorPro time is up 241.4% (last_30_days)."
    into:
      "Zach Williams AdvisorPro time is up 241.4% in the last month."
    """

    if not raw_message:
        return raw_message

    pretty_window = WINDOW_PRETTY.get(window_label, window_label)

    # remove "(last_X_days)" and any trailing period
    msg = re.sub(r"\s*\(last_\d+_days\)\.?\s*$", "", raw_message).strip()

    return f"{msg} in {pretty_window}."


def save_all_candidates(conn, run_id, mgr_id, window_label, start_dt, end_dt, all_candidates):
    cur = conn.cursor()

    rows = []
    for c in all_candidates:
        rows.append((
            run_id,
            mgr_id,
            window_label,
            c["polarity"],
            c["user_id"],
            c["insight_type"],
            c.get("raw_title"),
            prettify_message(c.get("raw_message"), window_label),
            c.get("severity_score", 0),
            start_dt,
            end_dt
        ))

    cur.executemany("""
        INSERT INTO ai_insight_candidates
        (run_id, manager_id, window_label, polarity, user_id, insight_type, raw_title, raw_message, severity_score, start_date, end_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, rows)

    conn.commit()
    cur.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manager_id", type=int, required=True)
    args = parser.parse_args()
    run_for_manager(args.manager_id)
