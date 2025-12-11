# scorecard_api.py
from flask import request, Blueprint, jsonify, session, current_app, make_response
from flask_login import login_required
from datetime import datetime
from pytz import timezone, utc
import os
import mysql.connector

scorecard_api = Blueprint('scorecard_api', __name__)

# ---------- DB ----------
def get_db_connection():
    cfg = current_app.config.get('MYSQL_CONFIG')
    if cfg:
        return mysql.connector.connect(**cfg)
    # Fallback env (rarely used)
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "mysql.railway.internal"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "railway"),
        port=int(os.getenv("DB_PORT", "3306")),
    )

# ---------- Helpers ----------
def nocache(json_payload):
    """Wrap jsonify payload with no-store headers to avoid cross-session caching."""
    resp = make_response(jsonify(json_payload))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp

def grade_from_total(score_val: float | int):
    """Turn a numeric total_score (0-100) into a letter + color."""
    try:
        s = float(score_val)
    except:
        s = 0.0

    if s >= 90:  return "A", "#0f0"
    elif s >= 80: return "B", "#ff0"
    elif s >= 70: return "C", "#f90"
    elif s >= 60: return "D", "#f60"
    else:         return "F", "#f00"

def pacific_window_for_date(date_str: str | None):
    pacific = timezone("US/Pacific")
    if date_str:
        target = pacific.localize(datetime.strptime(date_str, "%Y-%m-%d"))
    else:
        target = datetime.now(pacific)
    start = target.replace(hour=0, minute=0, second=0, microsecond=0)
    end   = target.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start.astimezone(utc), end.astimezone(utc), start.date()

# ---------- SCORECARD ----------
@scorecard_api.route("/api/scorecard")
@login_required
def get_scorecard():
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return nocache({"error": "unauthorized"}), 401

    # try all the ways the UI might send the date
    date_str = (
        request.args.get("date")
        or request.args.get("from")
        or request.args.get("start")
    )
    try:
        _start_utc, _end_utc, pac_date = pacific_window_for_date(date_str)
        pac_date_str = pac_date.strftime("%Y-%m-%d")
    except Exception as e:
        return nocache({"error": f"Invalid date format: {e}"}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT
                u.id AS user_id,
                u.email,
                u.nickname,
                COALESCE(f.phone_activity_score, 0)     AS phone_activity_score,
                COALESCE(f.movement_activity_score, 0)  AS movement_activity_score,
                COALESCE(f.quote_activity_score, 0)     AS quote_activity_score,
                COALESCE(f.binary_vc_score, 0)          AS binary_vc_score,
                COALESCE(f.total_score, 0)              AS total_score
            FROM users u
            LEFT JOIN fact_daily_scores f
                   ON f.user_id = u.id
                  AND f.date = %s
            WHERE u.manager_id = %s
              AND u.role = 'user'
            ORDER BY COALESCE(NULLIF(u.nickname,''), u.email)
        """, (pac_date_str, mgr_id))
        rows = cur.fetchall() or []
    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

    results = []
    for r in rows:
        total = float(r.get("total_score") or 0)
        grade, color = grade_from_total(total)
        results.append({
            "id": r["user_id"],
            "nickname": r.get("nickname"),
            "email": r["email"],
            "label": (r.get("nickname") or "").strip() or r["email"],

            # 👇 THIS is the important line
            "score": total,                  # ← UI should show this
            "score_percent": int(round(total)),  # ← if UI wants 85%

            "grade": grade,
            "color": color,
            "phone_activity_score":     float(r.get("phone_activity_score") or 0),
            "movement_activity_score":  float(r.get("movement_activity_score") or 0),
            "quote_activity_score":     float(r.get("quote_activity_score") or 0),
            "binary_vc_score":          float(r.get("binary_vc_score") or 0),
        })

    return nocache(results)

# ---------- USER METRICS ----------
@scorecard_api.route("/api/user-metrics", methods=["GET"])
@login_required
def get_user_metrics():
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return nocache({"error": "unauthorized"}), 401

    user_email = request.args.get("user")       # legacy
    user_id    = request.args.get("user_id", type=int)  # preferred
    date_str   = request.args.get("date")
    if (not user_email and not user_id) or not date_str:
        return nocache({"error": "Missing user/user_id or date"}), 400

    try:
        start_utc, end_utc, pac_date = pacific_window_for_date(date_str)
    except Exception as e:
        return nocache({"error": f"Invalid date: {e}"}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # verify the user belongs to this manager
        if user_id:
            cur.execute("""
                SELECT id, email, nickname
                FROM users
                WHERE id=%s AND manager_id=%s AND role='user'
                LIMIT 1
            """, (user_id, mgr_id))
        else:
            cur.execute("""
                SELECT id, email, nickname
                FROM users
                WHERE email=%s AND manager_id=%s AND role='user'
                LIMIT 1
            """, (user_email, mgr_id))
        row = cur.fetchone()
        if not row:
            return nocache({"error": "User not found under this manager"}), 404
        uid = int(row["id"])

        # activity totals (Pacific day window, table stores UTC)
        cur.execute("""
            SELECT
              COALESCE(SUM(mouse_distance),0) AS mouse_distance,
              COALESCE(SUM(keystrokes),0)     AS keystrokes,
              COALESCE(SUM(mouse_clicks),0)   AS mouse_clicks,
              COALESCE(SUM(idle_count),0)     AS idle_count
            FROM activity_log
            WHERE user_id=%s
              AND `timestamp` >= %s
              AND `timestamp` <  %s
        """, (uid, start_utc, end_utc))
        act = cur.fetchone() or {}

        idle_minutes = round(float(act.get("idle_count") or 0) / 60.0, 2)

        # latest call snapshot for that Pacific calendar date
        cur.execute("""
            SELECT
                COALESCE(inbound_calls, 0)                   AS inbound_calls,
                COALESCE(outbound_calls, 0)                  AS outbound_calls,
                COALESCE(TIME_TO_SEC(inbound_time), 0)/60.0  AS in_talk,
                COALESCE(TIME_TO_SEC(outbound_time),0)/60.0  AS out_talk
            FROM call_metrics
            WHERE user_id = %s
              AND DATE(CONVERT_TZ(created_at, 'UTC', 'America/Los_Angeles')) = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (uid, pac_date))
        calls = cur.fetchone() or {"inbound_calls":0,"outbound_calls":0,"in_talk":0.0,"out_talk":0.0}

        resp = {
            "user_id": uid,
            "email": row.get("email"),
            "nickname": row.get("nickname"),
            "label": (row.get("nickname") or "").strip() or row.get("email"),
            "mouse_distance": float(act.get("mouse_distance") or 0),
            "keystrokes":     int(act.get("keystrokes") or 0),
            "mouse_clicks":   int(act.get("mouse_clicks") or 0),
            "idle_minutes":   idle_minutes,
            "idle_time":      idle_minutes,   # legacy alias
            "inbound":        int(calls.get("inbound_calls") or 0),
            "outbound":       int(calls.get("outbound_calls") or 0),
            "in_talk":        round(float(calls.get("in_talk") or 0), 2),
            "out_talk":       round(float(calls.get("out_talk") or 0), 2),
        }
        return nocache(resp)
    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

# ---------- TEAM SCORE ----------
@scorecard_api.route("/api/team-score", methods=["GET"])
@login_required
def team_score():
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return nocache({"error": "unauthorized"}), 401

    # try to read the date the same way the dashboard sends it
    date_str = (
        request.args.get("date")
        or request.args.get("from")
        or request.args.get("start")
    )

    # if the frontend didn't send a date, fall back to the latest date we have in fact_daily_scores
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    if not date_str:
        cur.execute("SELECT MAX(date) AS max_date FROM fact_daily_scores")
        row = cur.fetchone()
        if row and row["max_date"]:
            date_str = row["max_date"].strftime("%Y-%m-%d")

    try:
        _start_utc, _end_utc, pac_date = pacific_window_for_date(date_str)
        pac_date_str = pac_date.strftime("%Y-%m-%d")
    except Exception as e:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass
        return nocache({"error": f"Invalid date format: {e}"}), 400

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # 👉 NEW: read scores straight from fact_daily_scores
        cur.execute("""
            SELECT
                u.id AS user_id,
                u.email,
                u.nickname,
                COALESCE(f.total_score, 0)              AS total_score,
                COALESCE(f.phone_activity_score, 0)     AS phone_activity_score,
                COALESCE(f.movement_activity_score, 0)  AS movement_activity_score,
                COALESCE(f.quote_activity_score, 0)     AS quote_activity_score,
                COALESCE(f.binary_vc_score, 0)          AS binary_vc_score
            FROM users u
            LEFT JOIN fact_daily_scores f
                   ON f.user_id = u.id
                  AND f.date = %s
            WHERE u.manager_id = %s
              AND u.role = 'user'
            ORDER BY COALESCE(NULLIF(u.nickname,''), u.email)
        """, (pac_date_str, mgr_id))
        rows = cur.fetchall() or []
    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

    users_out = []
    all_scores = []
    grade_counts = {"A":0,"B":0,"C":0,"D":0,"F":0}

    for r in rows:
        total = float(r.get("total_score") or 0)
        grade, color = grade_from_total(total)
        label = (r.get("nickname") or "").strip() or r["email"]

        users_out.append({
            "id": r["user_id"],
            "nickname": r.get("nickname"),
            "email": r["email"],
            "label": label,
            "score": total,            # 👈 this is the same “Score” as UI
            "grade": grade,
            "color": color,
            # send subs if UI ever wants to show them
            "phone_activity_score":     float(r.get("phone_activity_score") or 0),
            "movement_activity_score":  float(r.get("movement_activity_score") or 0),
            "quote_activity_score":     float(r.get("quote_activity_score") or 0),
            "binary_vc_score":          float(r.get("binary_vc_score") or 0),
        })

        all_scores.append(total)
        if grade in grade_counts:
            grade_counts[grade] += 1

    def avg(lst):
        return int(round(sum(lst) / len(lst))) if lst else 0

    payload = {
        "date": pac_date_str,
        "manager_id": int(mgr_id),
        "total_users": len(rows),
        "team_score": avg(all_scores),   # 👈 avg of fact_daily_scores for that day
        "grades": grade_counts,
        "users": users_out,
    }
    return nocache(payload)
