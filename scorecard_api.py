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

    if s >= 85:  return "Excellent", "#00ff00"
    elif s >= 75: return "Above Average", "#00cc00"
    elif s >= 65: return "Average", "#ffff00"
    elif s >= 55: return "Below Average", "#ffa500"
    else:         return "Poor", "#ff0000"

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
                COALESCE(fd.mouse_distance, 0) AS mouse_distance,
                CASE
                    WHEN fd.mouse_distance IS NULL OR fd.mouse_distance = 0 THEN 1
                    ELSE 0
                END AS is_inactive,
                -- scores (only valid if active)
                CASE
                    WHEN fd.mouse_distance IS NULL OR fd.mouse_distance = 0 THEN NULL
                    ELSE COALESCE(f.total_score, 0)
                END AS total_score,
                CASE
                    WHEN fd.mouse_distance IS NULL OR fd.mouse_distance = 0 THEN NULL
                    ELSE COALESCE(f.phone_activity_score, 0)
                END AS phone_activity_score,
                CASE
                    WHEN fd.mouse_distance IS NULL OR fd.mouse_distance = 0 THEN NULL
                    ELSE COALESCE(f.movement_activity_score, 0)
                END AS movement_activity_score,
                CASE
                    WHEN fd.mouse_distance IS NULL OR fd.mouse_distance = 0 THEN NULL
                    ELSE COALESCE(f.quote_activity_score, 0)
                END AS quote_activity_score,
                CASE
                    WHEN fd.mouse_distance IS NULL OR fd.mouse_distance = 0 THEN NULL
                    ELSE COALESCE(f.binary_vc_score, 0)
                END AS binary_vc_score
            FROM users u
            LEFT JOIN fact_daily_scores f
                   ON f.user_id = u.id
                  AND f.date = %s
            LEFT JOIN fact_daily fd
                   ON fd.user_id = u.id
                  AND fd.date = %s
            WHERE u.manager_id = %s
              AND u.role = 'user'
            ORDER BY COALESCE(NULLIF(u.nickname,''), u.email)
        """, (pac_date_str, pac_date_str, mgr_id))
        rows = cur.fetchall() or []

    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

    results = []
    for r in rows:
        # if total_score was NULL (inactive), this becomes 0 — that’s fine,
        # the frontend will show "—" because we send is_inactive too
        total = float(r.get("total_score") or 0)
        grade, color = grade_from_total(total)

        results.append({
            "id": r["user_id"],
            "nickname": r.get("nickname"),
            "email": r["email"],
            "label": (r.get("nickname") or "").strip() or r["email"],

            "score": total,
            "score_percent": int(round(total)),
            "grade": grade,
            "color": color,

            "phone_activity_score":     float(r.get("phone_activity_score") or 0),
            "movement_activity_score":  float(r.get("movement_activity_score") or 0),
            "quote_activity_score":     float(r.get("quote_activity_score") or 0),
            "binary_vc_score":          float(r.get("binary_vc_score") or 0),

            # 👇 this is what the JS needs to gray it out
            "is_inactive": bool(r.get("is_inactive")),
            "mouse_distance": float(r.get("mouse_distance") or 0),
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

# ---------- TEAM SCORE (supports single day OR date range) ----------
@scorecard_api.route("/api/team-score", methods=["GET"])
@login_required
def team_score():
    from datetime import datetime, timedelta

    mgr_id = session.get("manager_id")
    if not mgr_id:
        return nocache({"error": "unauthorized"}), 401

    # 1) read dates from query
    # accepts ?date=, or ?start= & ?end=
    start_str = (
        request.args.get("start")
        or request.args.get("from")
        or request.args.get("date")
    )
    end_str = request.args.get("end")

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    # 2) if no date at all → use latest from fact_daily_scores
    if not start_str:
        cur.execute("SELECT MAX(date) AS max_date FROM fact_daily_scores")
        row = cur.fetchone()
        if row and row["max_date"]:
            start_str = row["max_date"].strftime("%Y-%m-%d")
            end_str = start_str  # single day
        else:
            # no data at all
            cur.close(); conn.close()
            return nocache({
                "start": None,
                "end": None,
                "manager_id": int(mgr_id),
                "total_users": 0,
                "days": 0,
                "team_score": 0,
            })

    # if caller sent start= but no end= → treat as single day
    if not end_str:
        end_str = start_str

    # 3) normalize to date objects
    try:
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date   = datetime.strptime(end_str,   "%Y-%m-%d").date()
    except ValueError as e:
        cur.close(); conn.close()
        return nocache({"error": f"Invalid date: {e}"}), 400

    # make sure start <= end
    if end_date < start_date:
        end_date = start_date

    # build list of dates in range
    dates = []
    d = start_date
    while d <= end_date:
        dates.append(d)
        d += timedelta(days=1)
    num_days = len(dates)

    # 4) get all users for this manager
    cur.execute("""
        SELECT id, email, nickname
        FROM users
        WHERE manager_id = %s
          AND role = 'user'
        ORDER BY COALESCE(NULLIF(nickname,''), email)
    """, (mgr_id,))
    users = cur.fetchall() or []
    user_ids = [u["id"] for u in users]

    if not users:
        cur.close(); conn.close()
        return nocache({
            "start": start_str,
            "end": end_str,
            "manager_id": int(mgr_id),
            "total_users": 0,
            "days": num_days,
            "team_score": 0,
        })

    # 5) pull ALL fact_daily_scores for that manager’s users in that date range
    # now also join fact_daily so we know if they actually used Reflexx (mouse_distance)
    format_ids = ",".join(["%s"] * len(user_ids))
    cur.execute(f"""
        SELECT
            s.user_id,
            s.date,
            COALESCE(s.total_score, 0)             AS total_score,
            COALESCE(s.phone_activity_score, 0)    AS phone_activity_score,
            COALESCE(s.movement_activity_score, 0) AS movement_activity_score,
            COALESCE(s.quote_activity_score, 0)    AS quote_activity_score,
            COALESCE(s.binary_vc_score, 0)         AS binary_vc_score,
            COALESCE(fd.mouse_distance, 0)         AS mouse_distance,
            CASE
                WHEN fd.mouse_distance IS NULL OR fd.mouse_distance = 0 THEN 1
                ELSE 0
            END AS is_inactive
        FROM fact_daily_scores s
        JOIN fact_daily fd
          ON fd.user_id = s.user_id
         AND fd.date    = s.date
        WHERE s.date BETWEEN %s AND %s
          AND s.user_id IN ({format_ids})
    """, (start_str, end_str, *user_ids))
    score_rows = cur.fetchall() or []

    cur.close()
    conn.close()

    # index by (user_id, date_str)
    score_map = {}
    for r in score_rows:
        d_str = r["date"].strftime("%Y-%m-%d")
        score_map[(r["user_id"], d_str)] = r

    # 6) now walk every (user × day) and average
    grand_total = 0.0
    observations = 0

    users_out = []

    for u in users:
        u_total_for_range = 0.0
        active_days_for_user = 0

        for d in dates:
            d_str = d.strftime("%Y-%m-%d")
            row = score_map.get((u["id"], d_str))

            # no row at all → skip (probably didn’t use Reflexx)
            if not row:
                continue

            # row exists but user was inactive (mouse_distance = 0) → skip
            if row.get("is_inactive") in (1, True, "1"):
                continue

            val = float(row.get("total_score") or 0)

            grand_total += val
            u_total_for_range += val
            observations += 1
            active_days_for_user += 1

        # user-level average should also only look at their active days
        if active_days_for_user:
            avg_for_user = u_total_for_range / active_days_for_user
        else:
            avg_for_user = 0.0

        users_out.append({
            "id": u["id"],
            "label": (u.get("nickname") or "").strip() or u["email"],
            "avg_score": round(avg_for_user, 2),
        })


    # avoid div by zero
    from decimal import Decimal, ROUND_HALF_UP

    team_score = 0
    if observations:
        avg = grand_total / observations
        team_score = int(Decimal(avg).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


    payload = {
        "start": start_date.strftime("%Y-%m-%d"),
        "end":   end_date.strftime("%Y-%m-%d"),
        "manager_id": int(mgr_id),
        "total_users": len(users),
        "days": num_days,
        "observations": observations,  # this should be users * days
        "team_score": team_score,
        "users": users_out,
    }
    return nocache(payload)
