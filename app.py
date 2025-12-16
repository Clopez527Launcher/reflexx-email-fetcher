import re
import os
import io
import json
import csv
from uuid import uuid4
from datetime import datetime, timedelta, date
import csv, io

import pytz
import openai
import pandas as pd
import mysql.connector
import pymysql  # keep if other parts of app use it; safe to leave

from werkzeug.utils import secure_filename
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
from flask import send_from_directory
from io import BytesIO

from flask import (
    Flask, request, jsonify, render_template, redirect, url_for,
    session, Response, send_from_directory, current_app
)
from flask_login import login_user  # keep if you use it elsewhere
from flask_login import current_user

# Blueprints
from scorecard_api import scorecard_api
from ai_routes import ai_bp

# Quotes parsing helpers
from quotes_utils import parse_quotes_excel, insert_into_quotes_raw_rows, _connect_from_env

# Timezones
PT = ZoneInfo("America/Los_Angeles")
UTC = ZoneInfo("UTC")


#from sales_daily_api import sales_daily_api
#app.register_blueprint(sales_daily_api)

# ✅ Flask Imports Above
from flask import Flask, request, jsonify, render_template, redirect, url_for, session, Response
from flask_cors import CORS

# ✅ Flask-Login for Authentication
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user

# ✅ Custom Modules (Ensure `weblogs_dashboard.py` exists)
from weblogs_dashboard import weblogs_bp

# ✅ Function Wrapping for Login Protection
from functools import wraps

# --- RingCentral token refresh (app.py) ---
import os, threading, time, requests

RC_CLIENT_ID = os.getenv("RC_CLIENT_ID")        # set in Railway
RC_CLIENT_SECRET = os.getenv("RC_CLIENT_SECRET")

rc_access_token = None
rc_refresh_token = None
rc_expires_at = 0
_rc_thread_started = False

RC_REFRESH_INTERVAL = 1800  # 30 min

# Helper: convert timedelta objects (and nested dict/list values) to strings
def _serialize_for_json(obj):
    from datetime import timedelta as _TD

    if isinstance(obj, _TD):
        # "HH:MM:SS" style string is fine for the UI
        return str(obj)

    if isinstance(obj, dict):
        return {k: _serialize_for_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_serialize_for_json(v) for v in obj]

    return obj


def refresh_ringcentral_token():
    global rc_access_token, rc_refresh_token, rc_expires_at
    while True:
        try:
            # Skip until we actually have a refresh token
            if not rc_refresh_token or not RC_CLIENT_ID or not RC_CLIENT_SECRET:
                time.sleep(5)
                continue

            print("🔄 Refreshing RingCentral token...")
            resp = requests.post(
                "https://platform.ringcentral.com/restapi/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": rc_refresh_token,
                    "client_id": RC_CLIENT_ID,
                    "client_secret": RC_CLIENT_SECRET,
                },
                timeout=15,
            )

            if resp.status_code == 200:
                t = resp.json()
                rc_access_token  = t["access_token"]
                rc_refresh_token = t["refresh_token"]
                rc_expires_at    = time.time() + int(t.get("expires_in", 3600))
                print("✅ RingCentral token refreshed")
            else:
                print(f"⚠️ Refresh failed: {resp.status_code} {resp.text}")

        except Exception as e:
            print(f"❌ Exception refreshing token: {e}")

        time.sleep(RC_REFRESH_INTERVAL)

def start_rc_refresher_thread_once():
    global _rc_thread_started
    if not _rc_thread_started:
        threading.Thread(target=refresh_ringcentral_token, daemon=True).start()
        _rc_thread_started = True

# after obtaining initial tokens:
start_rc_refresher_thread_once()

# ✅ Initialize Flask App
app = Flask(__name__)
CORS(app)  # Enables Cross-Origin Requests (needed for APIs)
app.secret_key = "supersecretkey"  # 🔥 Change this in production!
app.register_blueprint(scorecard_api)

# --- Flask-Login setup (paste this under app = Flask(__name__) etc.) ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"  # name of your login endpoint/function

# ===== Quotes upload storage =====
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
QUOTES_DIR = os.path.join(BASE_DIR, "uploads", "quotes")
os.makedirs(QUOTES_DIR, exist_ok=True)

ALLOWED_QUOTE_EXTS = {".xlsx", ".xls", ".csv"}


@login_manager.unauthorized_handler
def unauthorized():
    # If the user hits an API while logged out/expired, send JSON (not HTML)
    if request.path.startswith("/api/"):
        return jsonify({"error": "unauthorized", "message": "session expired"}), 401
    # normal pages still redirect to login
    return redirect(url_for("login"))
    
from datetime import timedelta

app.config.update(
    PERMANENT_SESSION_LIFETIME=timedelta(hours=8),   # last ~full workday
    SESSION_REFRESH_EACH_REQUEST=True,               # extends while user is active
    SESSION_COOKIE_SAMESITE="Lax",
    # turn on the next line in production over HTTPS:
    # SESSION_COOKIE_SECURE=True,
)    

@app.before_request
def make_session_permanent():
    session.permanent = True

# ✅ MySQL Configuration (Railway)
DB_HOST = "mysql.railway.internal"
DB_USER = "root"
DB_PASSWORD = "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc"  # Use your actual Railway password
DB_NAME = "railway"

MYSQL_CONFIG = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": 3306  # or your actual port if different
}

app.config['MYSQL_CONFIG'] = MYSQL_CONFIG


from sqlalchemy import text  # make sure this is near the top of app.py

from flask import request, jsonify
from datetime import datetime

@app.get("/api/buckets")
def get_buckets():
    bucket = request.args.get("bucket")
    start_raw = request.args.get("start")
    end_raw   = request.args.get("end")

    # map which card → which column on fact_daily_scores
    column_map = {
        "phone":   "phone_activity_score",
        "quoting": "quote_activity_score",
        "movement": "movement_activity_score",
    }
    col = column_map.get(bucket)
    if not col:
        return jsonify([])

    # mm/dd/yyyy -> yyyy-mm-dd
    def to_mysql_date(val):
        if not val:
            return None
        try:
            dt = datetime.strptime(val, "%m/%d/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return val

    start = to_mysql_date(start_raw)
    end   = to_mysql_date(end_raw)

    conn = get_db_connection()
    cursor = conn.cursor()  # your conn is returning dict-like rows

    # join fact_daily_scores to users so we can display names
    query = f"""
        SELECT
            u.nickname AS display_name,
            ROUND(SUM(f.{col}) / COUNT(DISTINCT f.date), 2) AS avg_score
        FROM fact_daily_scores f
        JOIN users u ON f.user_id = u.id
        WHERE f.date BETWEEN %s AND %s
        GROUP BY u.nickname
        ORDER BY avg_score DESC
        LIMIT 10;
    """

    cursor.execute(query, (start, end))
    rows = cursor.fetchall()

    results = []
    for r in rows:
        # r looks like {'display_name': 'Eman Nasr', 'avg_score': Decimal('12.50')}
        results.append({
            "user_name": r["display_name"],
            "avg_score": float(r["avg_score"]) if r["avg_score"] is not None else 0
        })

    cursor.close()
    conn.close()

    return jsonify(results)

@app.route("/api/reflexx_kpi")
@login_required
def api_reflexx_kpi():
    """
    Reflexx KPI / Index Leaderboard API

    RULES:
    - Always anchor to CALIFORNIA YESTERDAY (Pacific calendar yesterday), not server MAX(day) or UTC today.
    - For period="yesterday", use these columns:
        - daily_elite_per_minute  -> ratio/index
        - daily_elite_calls       -> elite_calls
        - daily_talk_seconds      -> talk_seconds
    - For 7d/14d/30d/60d, use the rolling window columns on that same anchor_day row:
        - w_7d_ratio, w_7d_elite_calls, w_7d_talk_seconds, etc.
    """
    conn = get_db_connection()
    cursor = conn.cursor()   # assumes dict-style rows from your helper
    
    # 🔐 Resolve manager_id safely (DB truth, no leakage)
    cursor.execute(
        "SELECT role, manager_id FROM users WHERE id = %s",
        (current_user.id,)
    )
    me = cursor.fetchone()

    # If I'm a manager, my manager_id is my own user id.
    # If I'm a user, my manager_id is stored in users.manager_id.
    manager_id = current_user.id if (me and me.get("role") == "manager") else (me.get("manager_id") if me else None)

    if manager_id is None:
        cursor.close()
        conn.close()
        return jsonify({"status": "error", "message": "Unable to resolve manager_id"}), 400


    # 1) Which window? (default 7d)
    period = request.args.get("period", "7d")

    # 2) Map period -> column names in elite_calls_master
    ratio_columns = {
        # yesterday is computed manually (no column)
        "yesterday": None,
        "7d": "w_7d_ratio",
        "14d": "w_14d_ratio",
        "30d": "w_30d_ratio",
        "60d": "w_60d_ratio",
    }

    calls_columns = {
        "yesterday": "daily_elite_calls",        # raw daily count
        "7d": "w_7d_elite_calls",
        "14d": "w_14d_elite_calls",
        "30d": "w_30d_elite_calls",
        "60d": "w_60d_elite_calls",
    }

    talk_columns = {
        "yesterday": "daily_talk_seconds",       # raw daily talk seconds
        "7d": "w_7d_talk_seconds",
        "14d": "w_14d_talk_seconds",
        "30d": "w_30d_talk_seconds",
        "60d": "w_60d_talk_seconds",
    }

    # Safety: if something weird comes in, default to 7d
    if period not in ratio_columns:
        period = "7d"

    ratio_col = ratio_columns[period]
    calls_col = calls_columns[period]
    talk_col = talk_columns[period]

    # 3) Anchor date: ALWAYS calendar yesterday in Pacific time (PST-ish)
    #    Railway runs in UTC, so we:
    #    - get current UTC time
    #    - shift back 8 hours to approximate Pacific
    #    - take that calendar date
    #    - then subtract 1 day for "yesterday"
    utc_now = datetime.utcnow()
    pst_like_now = utc_now - timedelta(hours=8)
    anchor_day = pst_like_now.date() - timedelta(days=1)

    # 4) Pull rows for ANCHOR DAY ONLY (the fully completed day in Pacific)
    if period == "yesterday":
        query = """
            SELECT
                user_id,
                user_name,
                daily_elite_calls AS elite_calls,
                daily_talk_seconds AS talk_seconds
            FROM elite_calls_fact_daily
            WHERE
                day = %s
                AND manager_id = %s
            ORDER BY daily_elite_calls DESC;
        """
    else:
        query = f"""
            SELECT 
                user_id,
                user_name,
                {ratio_col} AS ratio,
                {calls_col} AS elite_calls,
                {talk_col} AS talk_seconds
            FROM elite_calls_fact_daily
            WHERE
                day = %s
                AND manager_id = %s
            ORDER BY {ratio_col} DESC;
        """

    cursor.execute(query, (anchor_day, manager_id))
    rows = cursor.fetchall()

    # 5) Build leaderboard + compute team average index (ratio * 100)
    leaderboard = []

    sum_index_scores = 0.0
    count_users = 0

    # Adjusted Index uses SUM(calls) / SUM(minutes) * 100
    sum_calls = 0.0
    sum_talk_seconds = 0.0

    for r in rows:
        # ----- Per-user INDEX (ratio * 100) -----
        if period == "yesterday":
            calls = float(r["elite_calls"]) if r["elite_calls"] else 0.0
            talk_seconds = float(r["talk_seconds"]) if r["talk_seconds"] else 0.0
            ratio_value = (calls / (talk_seconds / 60.0)) if talk_seconds > 0 else 0.0
        else:
            raw_ratio = r["ratio"]
            ratio_value = float(raw_ratio) if raw_ratio is not None else 0.0

        index_score = ratio_value * 100

        # ----- Per-user calls + talk seconds (window-specific) -----
        calls = float(r["elite_calls"]) if r["elite_calls"] is not None else 0.0
        talk_seconds = float(r["talk_seconds"]) if r["talk_seconds"] is not None else 0.0
        talk_minutes = (talk_seconds / 60.0) if talk_seconds > 0 else 0.0

        leaderboard.append({
            "user_id": r["user_id"],
            "user_name": r["user_name"],
            "index": round(index_score, 1),

            # ✅ send these so JS can show details if you want
            "elite_calls": calls,
            "talk_seconds": talk_seconds,
            "talk_minutes": round(talk_minutes, 1),
        })

        sum_index_scores += index_score
        count_users += 1

        # ✅ THIS is the important part: totals for the chosen window
        sum_calls += calls
        sum_talk_seconds += talk_seconds

    team_index_avg = (sum_index_scores / count_users) if count_users > 0 else 0.0

    team_minutes = (sum_talk_seconds / 60.0) if sum_talk_seconds > 0 else 0.0
    team_adjusted_index = (sum_calls / team_minutes) * 100 if team_minutes > 0 else 0.0
    
    cursor.close()
    conn.close()

    return jsonify({
        "status": "ok",
        "period": period,
        "anchor_day": anchor_day.isoformat(),  # for debugging
        "rows": leaderboard,
        "team_index_avg": team_index_avg,
        "team_adjusted_index": team_adjusted_index
    })



@app.route("/api/elite_daily_index")
@login_required
def api_elite_daily_index():
    """
    Paginated list of daily elite-per-minute scores for the last 30 days
    (anchored to California "yesterday" in Pacific time).

    Returns JSON:
    {
      status: "ok",
      page: 1,
      page_size: 25,
      total_rows: N,
      total_pages: M,
      rows: [
        { "day": "2025-12-10", "user_id": 7, "user_name": "Jocelyn", "score": 0.23 },
        ...
      ]
    }
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 🔐 Resolve manager_id safely (DB truth, no leakage)
    cursor.execute(
        "SELECT role, manager_id FROM users WHERE id = %s",
        (current_user.id,)
    )
    me = cursor.fetchone()

    # If I'm a manager, my manager_id is my own user id.
    # If I'm a user, my manager_id is stored in users.manager_id.
    manager_id = current_user.id if (me and me.get("role") == "manager") else (me.get("manager_id") if me else None)

    if manager_id is None:
        cursor.close()
        conn.close()
        return jsonify({"status": "error", "message": "Unable to resolve manager_id"}), 400


    # Pagination inputs
    try:
        page = int(request.args.get("page", "1"))
    except ValueError:
        page = 1

    try:
        page_size = int(request.args.get("page_size", "25"))
    except ValueError:
        page_size = 25

    if page < 1:
        page = 1
    # Hard cap page size so nobody accidentally asks for huge pages
    if page_size < 1:
        page_size = 25
    if page_size > 100:
        page_size = 100

    # Anchor to California yesterday (Pacific-ish)
    utc_now = datetime.utcnow()
    pst_like_now = utc_now - timedelta(hours=8)
    anchor_day = pst_like_now.date() - timedelta(days=1)

    # 30-day window including anchor_day
    start_day = anchor_day - timedelta(days=29)

    # 1) Count total rows in the window (for pagination)
    count_sql = """
        SELECT COUNT(*) AS cnt
        FROM elite_calls_fact_daily
        WHERE day BETWEEN %s AND %s
          AND manager_id = %s
    """
    cursor.execute(count_sql, (start_day, anchor_day, manager_id))
    count_row = cursor.fetchone()
    total_rows = count_row["cnt"] if count_row and "cnt" in count_row else 0

    if total_rows == 0:
        cursor.close()
        conn.close()
        return jsonify({
            "status": "ok",
            "page": page,
            "page_size": page_size,
            "total_rows": 0,
            "total_pages": 1,
            "rows": []
        })

    # Calculate pages
    total_pages = (total_rows + page_size - 1) // page_size
    if page > total_pages:
        page = total_pages

    offset = (page - 1) * page_size

    # 2) Pull page of rows ordered by day DESC, then score DESC
    data_sql = """
        SELECT
            day,
            user_id,
            user_name,
            CASE
                WHEN daily_talk_seconds > 0
                    THEN (daily_elite_calls * 60.0 / daily_talk_seconds)
                ELSE 0
            END AS daily_elite_per_minute
        FROM elite_calls_fact_daily
        WHERE day BETWEEN %s AND %s
          AND manager_id = %s
        ORDER BY day DESC, daily_elite_per_minute DESC
        LIMIT %s OFFSET %s
    """

    cursor.execute(
        data_sql,
        (start_day, anchor_day, manager_id, page_size, offset)
    )

    rows = cursor.fetchall()

    result_rows = []
    for r in rows:
        val = r["daily_elite_per_minute"]
        score = float(val) if val is not None else 0.0
        result_rows.append({
            "day": r["day"].isoformat() if hasattr(r["day"], "isoformat") else str(r["day"]),
            "user_id": r["user_id"],
            "user_name": r["user_name"],
            "score": round(score, 3)
        })

    cursor.close()
    conn.close()

    return jsonify({
        "status": "ok",
        "page": page,
        "page_size": page_size,
        "total_rows": total_rows,
        "total_pages": total_pages,
        "anchor_day": anchor_day.isoformat(),
        "rows": result_rows
    })


from openpyxl import Workbook
from flask import Response
from datetime import datetime, timedelta
import io

@app.route("/api/elite_daily_index_export")
@login_required
def api_elite_daily_index_export():
    """
    Export Excel with:
      Sheet 1: Elite Daily Index (daily elite-per-minute)
      Sheet 2: Raw fact_daily metrics (11 fields)
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # 🔐 Resolve manager_id safely
    cursor.execute(
        "SELECT role, manager_id FROM users WHERE id = %s",
        (current_user.id,)
    )
    me = cursor.fetchone()

    manager_id = current_user.id if (me and me.get("role") == "manager") else me.get("manager_id")
    if manager_id is None:
        cursor.close()
        conn.close()
        return jsonify({"status": "error", "message": "Unable to resolve manager_id"}), 400

    # Anchor to California yesterday
    utc_now = datetime.utcnow()
    pst_like_now = utc_now - timedelta(hours=8)
    anchor_day = pst_like_now.date() - timedelta(days=1)
    start_day = anchor_day - timedelta(days=29)

    wb = Workbook()

    # ======================================================
    # SHEET 1 — Elite Daily Index
    # ======================================================
    ws1 = wb.active
    ws1.title = "Elite Daily Index"

    ws1.append(["Day", "User ID", "User Name", "Daily Index"])

    elite_sql = """
        SELECT
            day,
            user_id,
            user_name,
            CASE
                WHEN daily_talk_seconds > 0
                    THEN (daily_elite_calls * 60.0 / daily_talk_seconds)
                ELSE 0
            END AS elite_per_minute
        FROM elite_calls_fact_daily
        WHERE day BETWEEN %s AND %s
          AND manager_id = %s
        ORDER BY day DESC, user_name ASC
    """
    cursor.execute(elite_sql, (start_day, anchor_day, manager_id))
    for r in cursor.fetchall():
        ws1.append([
            r["day"],
            r["user_id"],
            r["user_name"],
            round(float(r["elite_per_minute"] or 0), 4)
        ])

    # ======================================================
    # SHEET 2 — Raw Fact Daily (11 metrics)
    # ======================================================
    ws2 = wb.create_sheet(title="Fact Daily Raw")

    ws2.append([
        "date", "user_id", "user_name",
        "inbounds", "outbounds", "ib_time_minutes", "ob_time_minutes",
        "quoted_items", "quotes_unique", "advisor_pro_minutes",
        "mouse_distance", "keystrokes", "mouse_clicks", "idle_time_seconds"
    ])

    fact_sql = """
        SELECT
            fd.date,
            fd.user_id,
            fd.user_name,
            fd.inbounds,
            fd.outbounds,
            fd.ib_time_minutes,
            fd.ob_time_minutes,
            fd.quoted_items,
            fd.quotes_unique,
            fd.advisor_pro_minutes,
            fd.mouse_distance,
            fd.keystrokes,
            fd.mouse_clicks,
            fd.idle_time_seconds
        FROM fact_daily fd
        JOIN users u ON u.id = fd.user_id
        WHERE fd.date BETWEEN %s AND %s
          AND u.manager_id = %s
        ORDER BY fd.date DESC, fd.user_name ASC
    """
    cursor.execute(fact_sql, (start_day, anchor_day, manager_id))
    for r in cursor.fetchall():
        ws2.append([
            r["date"],
            r["user_id"],
            r["user_name"],
            r["inbounds"],
            r["outbounds"],
            r["ib_time_minutes"],
            r["ob_time_minutes"],
            r["quoted_items"],
            r["quotes_unique"],
            r["advisor_pro_minutes"],
            r["mouse_distance"],
            r["keystrokes"],
            r["mouse_clicks"],
            r["idle_time_seconds"],
        ])

    cursor.close()
    conn.close()

    # Write workbook to memory
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    return Response(
        output.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=reflexx_daily_data.xlsx"
        }
    )


# ---------- Quotes: upload, list, view ----------
import io, os, time
from uuid import uuid4
from datetime import datetime, date
import pandas as pd
from flask import request, jsonify, send_from_directory, current_app, session
from flask_login import login_required, current_user
from ask_reflexx_ai import handle_ask_reflexx
from werkzeug.utils import secure_filename

from quotes_utils import parse_quotes_excel, insert_into_quotes_raw_rows, _connect_from_env

def _new_batch_id() -> int:
    return int(time.time() * 1000)

@app.route("/api/quotes/upload", methods=["POST"])
@login_required
def quotes_upload():
    # --- basic file checks ---
    file = request.files.get("file")
    if not file or file.filename.strip() == "":
        return jsonify({"error": "No file"}), 400

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_QUOTE_EXTS:
        return jsonify({"error": f"Extension {ext} not allowed"}), 400

    # --- persist uploaded file for audit/download ---
    original = secure_filename(file.filename)
    stored = f"{uuid4().hex}{ext}"
    path = os.path.join(QUOTES_DIR, stored)
    os.makedirs(QUOTES_DIR, exist_ok=True)
    file.save(path)

    size = os.path.getsize(path)
    mime = file.mimetype or ""

    # session manager_id is used to scope visibility on Business Metrics page
    mgr_id = session.get("manager_id")
    uid = session.get("user_id") or getattr(current_user, "id", None)

    cn = get_db_connection()
    cur = cn.cursor()
    cur.execute("""
        INSERT INTO quote_reports (manager_id, user_id, original_name, stored_name, mime_type, file_size)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (mgr_id, uid, original, stored, mime, size))
    cn.commit()
    cur.close(); cn.close()

    print(f"[quotes][upload] saved report original={original} stored={stored} size={size} mime={mime} ext={ext} path={path} mgr_id={mgr_id}")

    # === NEW PIPELINE: RAW INSERT -> STORED PROCS -> DAILY/FACT ===
    print("[quotes][upload] starting rollup hook…")
    try:
        # 1) Parse the just-saved file into a normalized DataFrame
        with open(path, "rb") as fh:
            buf = io.BytesIO(fh.read())
        df = parse_quotes_excel(buf)
        if df.empty:
            print("[quotes][upload] parsed_rows=0 (no valid rows)")
            return jsonify({"success": True, "note": "Parsed 0 rows"}), 200

        # 2) Insert raw into quotes_raw_rows with a new batch id
        batch_id = _new_batch_id()
        conn = _connect_from_env()  # single connection for the rest
        inserted = insert_into_quotes_raw_rows(df, report_id=batch_id, conn=conn)

        # 3) Determine date window and run stored procedures for that range
        dmin = df["production_date"].min()
        dmax = df["production_date"].max()
        print(f"[quotes][upload] parsed_rows={len(df)} day_range={dmin}..{dmax} inserted_raw={inserted} batch={batch_id}")

        curp = conn.cursor()
        curp.execute("CALL aggregate_quotes_daily(%s, %s)", (dmin, dmax))
        curp.execute("CALL sync_fact_daily_quotes(%s, %s)", (dmin, dmax))
        conn.commit()
        curp.close(); conn.close()

        return jsonify({
            "success": True,
            "inserted": int(inserted),
            "batch_id": batch_id,
            "from": str(dmin),
            "to": str(dmax)
        }), 200

    except Exception as e:
        print(f"[quotes][upload] ERROR processing rollup: {e}")
        # Non-fatal for the upload record itself
        return jsonify({"success": True, "warning": f"upload saved but rollup failed: {e}"}), 200
    # === end new pipeline ===


@app.route("/api/quotes/list", methods=["GET"])
@login_required
def quotes_list():
    mgr_id = session.get("manager_id")

    # --- pagination ---
    try:
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1
    per_page = 5
    offset = (page - 1) * per_page

    cn = get_db_connection()
    cur = cn.cursor()

    # total rows
    cur.execute("""
        SELECT COUNT(*) AS cnt
        FROM quote_reports
        WHERE (%s IS NULL AND manager_id IS NULL) OR manager_id = %s
    """, (mgr_id, mgr_id))
    total_row = cur.fetchone()

    # Be friendly to any DB driver: tuple/list row, dict-like row, or scalar
    if isinstance(total_row, (tuple, list)):
        total = int(total_row[0])
    elif hasattr(total_row, "keys"):
        total = int(total_row.get("cnt") or next(iter(total_row.values())))
    else:
        try:
            total = int(total_row)
        except Exception:
            total = 0

    # page rows
    cur.execute("""
        SELECT id, original_name, uploaded_at, file_size
        FROM quote_reports
        WHERE (%s IS NULL AND manager_id IS NULL) OR manager_id = %s
        ORDER BY uploaded_at DESC
        LIMIT %s OFFSET %s
    """, (mgr_id, mgr_id, per_page, offset))
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    items = []
    for r in rows:
        if hasattr(r, "keys"):            # dict-like row
            d = {k: r[k] for k in r.keys()}
        elif isinstance(r, (tuple, list)): # positional row
            d = dict(zip(cols, r))
        else:
            try:
                d = dict(r)
            except Exception:
                d = {}

        v = d.get("uploaded_at")
        if isinstance(v, datetime):
            dt = v
        elif isinstance(v, date):
            dt = datetime(v.year, v.month, v.day)
        else:
            dt = None
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)  # assume DB stored UTC-naive
            d["uploaded_at"] = dt.astimezone(PT).isoformat(timespec="seconds")
        else:
            d["uploaded_at"] = None

        try:
            d["file_size"] = int(d.get("file_size") or 0)
        except Exception:
            d["file_size"] = 0

        items.append(d)

    cur.close(); cn.close()

    total_pages = max(1, (total + per_page - 1) // per_page)

    return jsonify({
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": total_pages,
    })


@app.route("/quotes/view/<int:rid>", methods=["GET"])
@login_required
def quotes_view(rid):
    mgr_id = session.get("manager_id")

    cn = get_db_connection()
    cur = cn.cursor()
    cur.execute("""
        SELECT stored_name, original_name
        FROM quote_reports
        WHERE id=%s AND ((%s IS NULL AND manager_id IS NULL) OR manager_id=%s)
    """, (rid, mgr_id, mgr_id))
    row = cur.fetchone()
    cur.close(); cn.close()

    if not row:
        return "Not found", 404

    if hasattr(row, "keys"):
        stored_name = row["stored_name"]
        original_name = row["original_name"]
    elif isinstance(row, (tuple, list)):
        stored_name, original_name = row[0], row[1]
    else:
        try:
            d = dict(row)
            stored_name = d["stored_name"]; original_name = d["original_name"]
        except Exception:
            return "Not found", 404

    return send_from_directory(
        QUOTES_DIR,
        stored_name,
        as_attachment=False,
        download_name=original_name
    )

# ---------- Legacy endpoints, now routed through the new pipeline ----------

@app.route("/api/quotes/upload_and_rollup", methods=["POST"])
@login_required
def quotes_upload_and_rollup():
    # Delegate to the new upload pipeline
    return quotes_upload()


@app.route("/api/quotes/rollup_report/<int:rid>", methods=["GET", "POST"])
@login_required
def quotes_rollup_report(rid):
    """
    Re-process a stored quotes file by quote_reports.id using the new raw->procs pipeline.
    """
    # 1) Look up stored filename
    mgr_id = session.get("manager_id")
    try:
        mgr_id_int = int(mgr_id) if mgr_id is not None else None
    except:
        mgr_id_int = None

    cn = get_db_connection()
    cur = cn.cursor()
    cur.execute("""
        SELECT stored_name, original_name
        FROM quote_reports
        WHERE id=%s AND ((%s IS NULL AND manager_id IS NULL) OR manager_id=%s)
    """, (rid, mgr_id, mgr_id))
    row = cur.fetchone()
    cur.close(); cn.close()

    if not row:
        return jsonify({"error": "report not found or not visible for this manager"}), 404

    if hasattr(row, "keys"):
        stored_name = row["stored_name"]; original_name = row["original_name"]
    elif isinstance(row, (tuple, list)):
        stored_name, original_name = row[0], row[1]
    else:
        d = dict(row); stored_name = d["stored_name"]; original_name = d["original_name"]

    # 2) Parse
    file_path = os.path.join(QUOTES_DIR, stored_name)
    if not os.path.exists(file_path):
        return jsonify({"error": f"stored file not found on disk: {file_path}"}), 404

    with open(file_path, "rb") as fh:
        buf = io.BytesIO(fh.read())
    df = parse_quotes_excel(buf)
    parsed_rows = len(df)
    if parsed_rows == 0:
        return jsonify({"parsed_rows": 0, "note": "parser returned no rows"}), 200

    # 3) Insert raw + run procs for that file’s date range

    # try to extract the numeric id from the original filename, e.g.
    # "Quotes_Detail_Report__1761945842562.xlsx" -> 1761945842562
    report_id_for_rows = rid  # fallback to the DB id
    m = re.search(r"(\d+)", original_name or "")
    if m:
        try:
            report_id_for_rows = int(m.group(1))
        except ValueError:
            report_id_for_rows = rid  # just use db id if cast fails

    # figure out the date range in this file
    dmin = df["production_date"].min()
    dmax = df["production_date"].max()

    conn = _connect_from_env()
    inserted = insert_into_quotes_raw_rows(df, report_id=report_id_for_rows, conn=conn)

    # run your existing procs to push into daily tables
    curp = conn.cursor()
    curp.execute("CALL aggregate_quotes_daily(%s, %s)", (dmin, dmax))
    curp.execute("CALL sync_fact_daily_quotes(%s, %s)", (dmin, dmax))
    conn.commit()
    curp.close()
    conn.close()

    # 4) Summarize
    return jsonify({
        "reprocessed": True,
        "report_id": rid,
        "original_name": original_name,
        "stored_name": stored_name,
        "parsed_rows": int(parsed_rows),
        "raw_inserted": int(inserted),
        "from": str(dmin),
        "to": str(dmax),
        "uploaded_report_id_used": report_id_for_rows
    }), 200


# ✅ Download Only Screen
@app.route('/download_tracker')
def serve_tracker_file():
    return send_from_directory('static', 'ReflexxApp 3.1.1.exe', as_attachment=True, download_name='ReflexxApp 3.1.1.exe')
    
@app.route('/download_page')
def download_page():
    return render_template('download_only.html')
    

# ✅ Manager Portal Setup
@app.route('/manager')
def manager_portal():
    return render_template('manager_portal.html')  # Ensure this file exists
    
@app.route("/dashboard-new")
@login_required
def dashboard_new():
    # super simple for now
    return render_template("dashboard_new.html")
    
# Settings Page    
@app.route("/settings")
def settings():
    # ✅ Must be logged in
    user_id = session.get("user_id")
    if not user_id:
        return redirect("/login")

    return render_template("settings.html")
    
    
    
# ✅ Session Check Route (Step 2)
@app.route('/check-session')
def check_session():
    if 'user_id' in session:
        return 'OK', 200
    return 'EXPIRED', 401

# ✅ Pulls Agency Hours (close time)
@app.route("/api/get-agency-hours/<int:user_id>", methods=["GET"])
def get_agency_hours(user_id):
    def timedelta_to_str(tdelta):
        total_seconds = int(tdelta.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:02}"

    try:
        connection = get_db_connection()
        cursor = connection.cursor()  # This is fine because DictCursor is set globally in get_db_connection()
        query = """
            SELECT ah.work_start, ah.work_end
            FROM users u
            JOIN agency_hours ah ON u.manager_id = ah.agency_id
            WHERE u.id = %s
        """
        cursor.execute(query, (user_id,))
        row = cursor.fetchone()
        connection.close()

        print(f"DEBUG row = {row}")

        if row and 'work_start' in row and 'work_end' in row:
            result = {
                "work_start": timedelta_to_str(row['work_start']),
                "work_end": timedelta_to_str(row['work_end'])
            }
            print(f"✅ Agency hours fetched for user {user_id}: {result}")
            return jsonify(result)
        else:
            print(f"⚠️ No agency hours found for user {user_id}, returning fallback.")
            return jsonify({
                "work_start": "08:30:00",
                "work_end": "17:30:00"
            }), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Error in get_agency_hours: {e}")
        return jsonify({"error": "Internal server error"}), 500

# ✅ Analytics Page
# --- Analytics page + APIs (timezone-aware, scorecard-parity) ---

from flask import render_template, request, jsonify
from datetime import datetime, timedelta

# assumes get_db_connection() already exists

# Page route
@app.route('/analytics')
def analytics():
    return render_template('analytics.html')
    
# ---------- (Optional) tiny DB helper if you DON'T already have get_db_connection() ----------
try:
    get_db_connection  # if this exists, we'll just use it
except NameError:
    import mysql.connector
    def get_db_connection():
        return mysql.connector.connect(**MYSQL_CONFIG)

# ---------- Employees Dropdown (schema-agnostic) ----------
@app.route("/api/analytics/employees")
def api_employees():
    """
    Returns [{id, name}] for the Employee Trends dropdown.
    Auto-detects name-like columns on the users table to avoid 'unknown column' errors.
    """
    conn = get_db_connection()
    try:
        import pymysql
        cur = conn.cursor(pymysql.cursors.DictCursor)

        # Determine current database/schema name
        schema = None
        # Try common config vars first
        try:
            schema = MYSQL_CONFIG.get("database")  # if present
        except Exception:
            pass
        if not schema:
            schema = globals().get("DB_NAME")
        if not schema:
            # Fallback to SELECT DATABASE()
            cur.execute("SELECT DATABASE() AS db")
            schema = (cur.fetchone() or {}).get("db")

        # Discover available columns on users table
        cur.execute("""
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = 'users'
        """, (schema,))
        cols = {r["COLUMN_NAME"] for r in cur.fetchall()}

        # Build SELECT only with columns that exist
        fields = ["id"]
        for c in ["email", "name", "full_name", "display_name", "first_name", "last_name", "username", "is_active"]:
            if c in cols:
                fields.append(c)

        sql = "SELECT " + ", ".join(f"`{f}`" for f in fields) + " FROM `users`"
        if "is_active" in cols:
            sql += " WHERE `is_active` = 1"

        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        try: cur.close()
        except: pass
        conn.close()

    # Build a display name from whatever columns exist
    def pick_name(row):
        nz = lambda v: (v or "").strip()
        if "name" in row and nz(row["name"]): return nz(row["name"])
        if "full_name" in row and nz(row["full_name"]): return nz(row["full_name"])
        if "display_name" in row and nz(row["display_name"]): return nz(row["display_name"])
        if ("first_name" in row or "last_name" in row) and (nz(row.get("first_name")) or nz(row.get("last_name"))):
            return (f"{nz(row.get('first_name',''))} {nz(row.get('last_name',''))}").strip()
        if "username" in row and nz(row["username"]): return nz(row["username"])
        if "email" in row and nz(row["email"]): return nz(row["email"])
        return f"User #{row['id']}"

    out = [{"id": r["id"], "name": pick_name(r)} for r in rows]
    out.sort(key=lambda x: x["name"].lower())
    return jsonify(out)

# ---------- Employee time series (mouse/keys/clicks/idle) ----------
@app.route("/api/analytics/employee-series")
def api_employee_series():
    """
    Query params:
      user_id=<int>  start=YYYY-MM-DD  end=YYYY-MM-DD   (inclusive, Pacific)
    Returns daily sums for the 4 charts within that Pacific window.
    """
    user_id = request.args.get("user_id", type=int)
    start_s = request.args.get("start")
    end_s   = request.args.get("end")
    if not (user_id and start_s and end_s):
        return jsonify({"error": "missing user_id/start/end"}), 400

    pac = ZoneInfo("US/Pacific")
    utc = ZoneInfo("UTC")

    # Parse local (Pacific) days
    start_d = datetime.strptime(start_s, "%Y-%m-%d").date()
    end_d   = datetime.strptime(end_s,   "%Y-%m-%d").date()
    if end_d < start_d:
        start_d, end_d = end_d, start_d

    # Convert Pacific day window -> UTC datetimes for DB query
    start_utc = datetime.combine(start_d, datetime.min.time(), pac).astimezone(utc).replace(tzinfo=None)
    end_utc   = datetime.combine(end_d,   datetime.max.time(), pac).astimezone(utc).replace(tzinfo=None)

    # Pull raw rows
    conn = get_db_connection()
    try:
        import pymysql
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute("""
            SELECT timestamp, mouse_distance, keystrokes, mouse_clicks, idle_count
            FROM activity_log
            WHERE user_id = %s
              AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp
        """, (user_id, start_utc, end_utc))
        rows = cur.fetchall()
    finally:
        try: cur.close()
        except: pass
        conn.close()

    # Aggregate by Pacific-local day (no prefill)
    sums = {}  # { "YYYY-MM-DD": {"mouse":..., "keys":..., "clicks":..., "idle":...} }
    for r in rows:
        ts_utc = r["timestamp"].replace(tzinfo=utc)  # DB stored as UTC-naive
        ts_pac = ts_utc.astimezone(pac)
        key = ts_pac.date().strftime("%Y-%m-%d")
        s = sums.setdefault(key, {"mouse": 0.0, "keys": 0, "clicks": 0, "idle": 0})
        s["mouse"]  += float(r.get("mouse_distance") or 0)
        s["keys"]   += int(r.get("keystrokes") or 0)
        s["clicks"] += int(r.get("mouse_clicks") or 0)
        s["idle"]   += int(r.get("idle_count") or 0)

    # Keep only days that have any activity (> 0 across any metric)
    labels = []
    mouse  = []
    keys   = []
    clicks = []
    idle   = []

    for day in sorted(sums.keys()):
        s = sums[day]
        if (s["mouse"] + s["keys"] + s["clicks"] + s["idle"]) <= 0:
            continue  # skip zero-activity days (this naturally skips most weekends)
        labels.append(day)
        mouse.append(s["mouse"])
        keys.append(s["keys"])
        clicks.append(s["clicks"])
        idle.append(s["idle"])

    return jsonify({
        "labels": labels,
        "mouse_distance": mouse,
        "keystrokes": keys,
        "mouse_clicks": clicks,
        "idle_count": idle,
    })
       
# ---------- helpers ----------
def _rows_to_dicts(cur):
    rows = cur.fetchall()
    # If cursor is already dictionary=True
    if rows and isinstance(rows[0], dict):
        return rows
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

def _to_iso(d):
    try:
        return d.isoformat()
    except AttributeError:
        return str(d) if d is not None else None

# Use one timezone consistently for WHERE and GROUP BY
OFFICE_TZ = 'America/Los_Angeles'  # <-- change if needed
def _tz_expr():
    return f"CONVERT_TZ(`timestamp`, 'UTC', '{OFFICE_TZ}')"

from datetime import datetime, timedelta
from flask import request, jsonify, session

# ---------- /api/analytics/team-percent ----------
@app.route("/api/analytics/team-percent", methods=["GET"])
def team_percent():
    """
    Single-day team percent based on fact_daily_scores.total_score
    but ignore users who had no Reflexx activity (mouse_distance = 0).
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401
    try:
        mgr_id = int(mgr_id)
    except Exception:
        return jsonify({"error": "unauthorized"}), 401

    date_str = (
        request.args.get("date")
        or request.args.get("from")
        or request.args.get("start")
    )

    conn = cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        # if no date passed, use latest date
        if not date_str:
            cur.execute("SELECT MAX(date) AS max_date FROM fact_daily_scores")
            row = cur.fetchone()
            if not row or not row["max_date"]:
                return jsonify({"error": "no data"}), 200
            date_str = row["max_date"].strftime("%Y-%m-%d")

        # pull users + scores + activity for that date
        cur.execute("""
            SELECT
                u.id AS user_id,
                COALESCE(f.total_score, 0) AS total_score,
                fd.mouse_distance
            FROM users u
            LEFT JOIN fact_daily_scores f
                   ON f.user_id = u.id
                  AND f.date = %s
            LEFT JOIN fact_daily fd
                   ON fd.user_id = u.id
                  AND fd.date = %s
            WHERE u.manager_id = %s
              AND u.role = 'user'
        """, (date_str, date_str, mgr_id))
        rows = cur.fetchall() or []

        # only count users who actually had Reflexx running
        active_scores = []
        for r in rows:
            md = r.get("mouse_distance")
            if md is not None and float(md) > 0:
                active_scores.append(float(r["total_score"] or 0))

        if active_scores:
            team_pct = round(sum(active_scores) / len(active_scores), 2)
        else:
            team_pct = None

        return jsonify({
            "date": date_str,
            "team_percent": team_pct,
            "labels": [date_str],
            "data": [team_pct] if team_pct is not None else []
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cur: cur.close()
        finally:
            if conn: conn.close()

from datetime import datetime
from zoneinfo import ZoneInfo  # built-in on Python 3.9+

# ---------- /api/ce-buckets ----------
@app.route("/api/ce-buckets", methods=["GET"])
@login_required
def ce_buckets():
    """
    Returns latest-day CE z-scores per employee for this manager.
    Uses fact_daily columns:
      phone_ce_l7_z, quote_ce_l7_z, movement_ce_l7_z
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    try:
        mgr_id_int = int(mgr_id)
    except Exception:
        return jsonify({"error": "unauthorized"}), 401

    conn = mysql.connector.connect(**app.config["MYSQL_CONFIG"])
    cur = conn.cursor(dictionary=True)

    try:
        # 1) Find the latest *completed* CE day for this manager
        #    - must be BEFORE *today in Pacific* (no partial "today" data)
        #    - must have some CE raw data (phone OR quote OR movement)

        pacific_today = datetime.now(ZoneInfo("America/Los_Angeles")).date()

        cur.execute("""
            SELECT MAX(fd.date) AS latest_date
            FROM fact_daily fd
            JOIN users u ON u.id = fd.user_id
            WHERE u.manager_id = %s
              AND fd.date < %s
              AND (
                    fd.phone_ce_raw    IS NOT NULL
                 OR fd.quote_ce_raw    IS NOT NULL
                 OR fd.movement_ce_raw IS NOT NULL
              )
        """, (mgr_id_int, pacific_today))

        row = cur.fetchone()
        latest_date = row["latest_date"] if row else None


        if not latest_date:
            return jsonify({"date": None, "rows": []})

        # 2) Get all employees + their z-scores for that date
        cur.execute("""
            SELECT
                fd.user_id,
                COALESCE(u.nickname, u.email) AS user_name,

                -- PHONE Zs
                fd.phone_ce_l7_z,
                fd.phone_ce_l30_z,
                fd.phone_ce_l60_z,

                -- QUOTE Zs
                fd.quote_ce_l7_z,
                fd.quote_ce_l30_z,
                fd.quote_ce_l60_z,

                -- MOVEMENT Zs
                fd.movement_ce_l7_z,
                fd.movement_ce_l30_z,
                fd.movement_ce_l60_z
            FROM fact_daily fd
            JOIN users u ON u.id = fd.user_id
            WHERE u.manager_id = %s
              AND fd.date = %s
            ORDER BY COALESCE(u.nickname, u.email)
        """, (mgr_id_int, latest_date))


        rows = cur.fetchall()

        return jsonify({
            "date": latest_date.strftime("%Y-%m-%d"),
            "rows": rows
        })

    finally:
        cur.close()
        conn.close()

# ---------- /api/analytics/team-series ----------
@app.route("/api/analytics/team-series", methods=["GET"])
def team_series():
    """
    Returns series for a date range, using fact_daily_scores.total_score
    so charts/donut match the scorecard.
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401
    try:
        mgr_id = int(mgr_id)
    except Exception:
        return jsonify({"error": "unauthorized"}), 401

    start_str = request.args.get("start")
    end_str   = request.args.get("end")
    if not (start_str and end_str):
        return jsonify({"error": "provide ?start=YYYY-MM-DD&end=YYYY-MM-DD"}), 400

    try:
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end   = datetime.strptime(end_str,   "%Y-%m-%d").date()
        if start > end:
            start, end = end, start
    except ValueError:
        return jsonify({"error": "invalid date format"}), 400

    # ----- user filter (robust) -----
    raw_users = request.args.get("users")
    user_ids: list[int] = []
    if raw_users:
      for piece in raw_users.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if piece.lower() in ("all", "none", "null"):
            continue
        try:
            user_ids.append(int(piece))
        except ValueError:
            continue

    conn = cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        labels = []
        data = []

        day = start
        while day <= end:
            day_str = day.strftime("%Y-%m-%d")

            if user_ids:
                placeholders = ",".join(["%s"] * len(user_ids))
                cur.execute(f"""
                    SELECT
                        COALESCE(f.total_score, 0) AS total_score
                    FROM users u
                    LEFT JOIN fact_daily_scores f
                           ON f.user_id = u.id
                          AND f.date = %s
                    WHERE u.manager_id = %s
                      AND u.role = 'user'
                      AND u.id IN ({placeholders})
                """, (day_str, mgr_id, *user_ids))
            else:
                cur.execute("""
                    SELECT
                        COALESCE(f.total_score, 0) AS total_score
                    FROM users u
                    LEFT JOIN fact_daily_scores f
                           ON f.user_id = u.id
                          AND f.date = %s
                    WHERE u.manager_id = %s
                      AND u.role = 'user'
                """, (day_str, mgr_id))

            rows = cur.fetchall() or []
            scores = [float(r["total_score"] or 0) for r in rows]
            day_avg = round(sum(scores)/len(scores), 2) if scores else None

            labels.append(day_str)
            data.append(day_avg)

            day += timedelta(days=1)

        non_null = [v for v in data if v is not None]
        baseline_selected = round(sum(non_null)/len(non_null), 2) if non_null else None

        return jsonify({
            "labels": labels,
            "data": data,
            "baseline_selected": baseline_selected,
            "baseline_all_time": baseline_selected
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            if cur: cur.close()
        finally:
            if conn: conn.close()

# ---------- /api/analytics/advisor-pro-series ----------
@app.route("/api/analytics/advisor-pro-series", methods=["GET"])
def advisor_pro_series():
    """
    Returns 7-day Advisor Pro totals.
    Default: team totals for the manager.
    If employee_id is provided: totals for that user only.
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    employee_id = request.args.get("employee_id", "all")

    conn = mysql.connector.connect(**app.config["MYSQL_CONFIG"])
    cur = conn.cursor()

    # Build dynamic WHERE
    params = [mgr_id]
    where_user = ""
    if employee_id != "all":
        where_user = "AND u.id = %s"
        params.append(employee_id)

    query = f"""
        SELECT 
            f.date,
            SUM(f.advisor_pro_minutes) AS minutes
        FROM fact_daily_scores f
        JOIN users u ON u.id = f.user_id
        WHERE u.manager_id = %s
          {where_user}
          AND f.date >= CURDATE() - INTERVAL 7 DAY
          AND f.date < CURDATE()
        GROUP BY f.date
        ORDER BY f.date ASC
    """

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Format results
    data = []
    for row in rows:
        d = row[0]
        minutes = row[1] or 0
        data.append({
            "date": d.strftime("%m/%d"),
            "weekday": d.strftime("%a"),
            "minutes": minutes
        })

    return jsonify({"data": data})

# ---------- /api/buckets ----------
@app.route("/api/buckets", methods=["GET"])
def buckets():
    """
    Returns the leaderboard for phone / quoting / movement
    over a date range.
    Frontend calls: /api/buckets?bucket=phone&start=YYYY-MM-DD&end=YYYY-MM-DD
    """
    bucket = request.args.get("bucket")
    start_str = request.args.get("start")
    end_str   = request.args.get("end")

    if not (bucket and start_str and end_str):
        return jsonify([])

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    # we are using fact_daily_scores like you showed
    if bucket == "phone":
        sql = """
            SELECT
                f.user_id,
                u.user_name,
                AVG(f.phone_activity_score) AS avg_score
            FROM fact_daily_scores AS f
            LEFT JOIN users AS u ON u.id = f.user_id
            WHERE f.date BETWEEN %s AND %s
            GROUP BY f.user_id, u.user_name
            ORDER BY avg_score DESC
            LIMIT 10
        """
    elif bucket == "quoting":
        sql = """
            SELECT
                f.user_id,
                u.user_name,
                AVG(f.quote_activity_score) AS avg_score
            FROM fact_daily_scores AS f
            LEFT JOIN users AS u ON u.id = f.user_id
            WHERE f.date BETWEEN %s AND %s
            GROUP BY f.user_id, u.user_name
            ORDER BY avg_score DESC
            LIMIT 10
        """
    elif bucket == "movement":
        sql = """
            SELECT
                f.user_id,
                u.user_name,
                AVG(f.movement_activity_score) AS avg_score
            FROM fact_daily_scores AS f
            LEFT JOIN users AS u ON u.id = f.user_id
            WHERE f.date BETWEEN %s AND %s
            GROUP BY f.user_id, u.user_name
            ORDER BY avg_score DESC
            LIMIT 10
        """
    else:
        cur.close()
        conn.close()
        return jsonify([])

    cur.execute(sql, (start_str, end_str))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify(rows)

# ---------- Ask Reflexx AI ---------
@app.route("/api/ask_reflexx_ai", methods=["POST"])
@login_required
def api_ask_reflexx_ai():
    """
    Backend for the Ask Reflexx AI widget.
    Expects JSON:
      {
        "question": "Who put forth the most effort this week and why?",
        "start": "2025-12-01",   # optional; if missing, defaults to last 7 days
        "end":   "2025-12-07"    # optional
      }
    """
    data = request.get_json(silent=True) or {}

    question = (data.get("question") or "").strip()
    # We’ll ignore custom start/end for now and let the backend
    # default to “last 7 days”. We can wire these up later.
    # start_ymd = data.get("start") or None
    # end_ymd   = data.get("end") or None

    manager_id = getattr(current_user, "manager_id", 0) or 0

    conn = get_db_connection()
    try:
        answer = handle_ask_reflexx(
            question=question,
            db_conn=conn,
            manager_id=manager_id,
            # start_date=None,
            # end_date=None,
        )
    finally:
        conn.close()

    return jsonify({"answer": answer})


# ---------- /api/buckets/phone-detail ----------
@app.route("/api/buckets/phone-detail", methods=["GET"])
def bucket_phone_detail():
    from datetime import datetime

    # params from query string
    user_id   = request.args.get("user_id", type=int)
    user_name = request.args.get("user_name")
    start_str = request.args.get("start")
    end_str   = request.args.get("end")

    # DEBUG: see what the frontend actually sent
    print("[phone-detail] user_id:", user_id, "user_name:", user_name,
          "start:", start_str, "end:", end_str, flush=True)

    # dates required
    if not (start_str and end_str):
        return jsonify({"error": "Missing date(s)"}), 400

    # helper to return zeros
    def empty_payload(label):
        return {
            "user_id": user_id,
            "user_name": label,
            "inbounds": 0,
            "outbounds": 0,
            "ib_time_minutes": 0,
            "ob_time_minutes": 0,
            "start": start_str,
            "end": end_str
        }

    # parse dates
    try:
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end   = datetime.strptime(end_str, "%Y-%m-%d").date()
    except Exception:
        return jsonify({"error": "Invalid date format"}), 400

    conn = get_db_connection()
    cur  = conn.cursor()   # <-- no dictionary=True

    try:
        # 1) if we only got a name, look up the id in users.nickname
        if not user_id and user_name:
            cur.execute(
                """
                SELECT id, nickname
                FROM users
                WHERE nickname = %s OR email = %s
                LIMIT 1
                """,
                (user_name, user_name)
            )
            row = cur.fetchone()
            if row:
                # row might be a dict OR a tuple, so handle both
                if isinstance(row, dict):
                    user_id   = row.get("id")
                    user_name = row.get("nickname") or user_name
                else:
                    # assume tuple: (id, nickname)
                    user_id   = row[0]
                    user_name = row[1]


        # 2) if STILL no id -> return zeros
        if not user_id:
            cur.close()
            conn.close()
            return jsonify(empty_payload(user_name or "Unknown"))

        # 3) aggregate from fact_daily_scores
        cur.execute(
            """
            SELECT
              SUM(COALESCE(inbounds, 0))        AS inbounds,
              SUM(COALESCE(outbounds, 0))       AS outbounds,
              SUM(COALESCE(ib_time_minutes, 0)) AS ib_time_minutes,
              SUM(COALESCE(ob_time_minutes, 0)) AS ob_time_minutes
            FROM fact_daily
            WHERE user_id = %s
              AND date BETWEEN %s AND %s
            """,
            (user_id, start, end)
        )
        sums = cur.fetchone()
        cur.close()
        conn.close()

        if not sums:
            return jsonify(empty_payload(user_name or "Unknown"))

        # sums is a dict in your setup, so read by column name
        if isinstance(sums, dict):
            inbounds        = int(sums.get("inbounds", 0) or 0)
            outbounds       = int(sums.get("outbounds", 0) or 0)
            ib_time_minutes = int(sums.get("ib_time_minutes", 0) or 0)
            ob_time_minutes = int(sums.get("ob_time_minutes", 0) or 0)
        else:
            # fallback if your driver ever returns a tuple
            inbounds        = int((sums[0] if len(sums) > 0 else 0) or 0)
            outbounds       = int((sums[1] if len(sums) > 1 else 0) or 0)
            ib_time_minutes = int((sums[2] if len(sums) > 2 else 0) or 0)
            ob_time_minutes = int((sums[3] if len(sums) > 3 else 0) or 0)

        return jsonify({
            "user_id": user_id,
            "user_name": user_name or f"User #{user_id}",
            "inbounds": inbounds,
            "outbounds": outbounds,
            "ib_time_minutes": ib_time_minutes,
            "ob_time_minutes": ob_time_minutes,
            "start": start_str,
            "end":   end_str
        })


    except Exception as e:
        import traceback
        print("[phone-detail ERROR]", e, flush=True)
        traceback.print_exc()
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        return jsonify(empty_payload(user_name or "Unknown")), 200

from datetime import datetime
from flask import request, jsonify

from datetime import datetime
from flask import request, jsonify

# ---------- /api/buckets/quoting-detail ----------
@app.route("/api/buckets/quoting-detail", methods=["GET"])
def bucket_quoting_detail():
    user_id   = request.args.get("user_id", type=int)
    user_name = request.args.get("user_name")
    start_str = request.args.get("start")
    end_str   = request.args.get("end")

    print("[quoting-detail] user_id:", user_id, "user_name:", user_name,
          "start:", start_str, "end:", end_str, flush=True)

    if not (start_str and end_str):
        return jsonify({"error": "Missing date(s)"}), 400

    try:
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end   = datetime.strptime(end_str, "%Y-%m-%d").date()
    except Exception:
        return jsonify({"error": "Invalid date format"}), 400

    conn = get_db_connection()
    cur  = conn.cursor()   # returns dict-like rows in your setup
    print("✅ Database Connection Established", flush=True)

    def empty_payload(label):
        return {
            "user_id": user_id,
            "user_name": label,
            "quoted_items": 0,
            "quotes_unique": 0,
            "advisor_pro_minutes": 0,
            "start": start_str,
            "end": end_str
        }

    try:
        # resolve by name if we didn't get an id
        if not user_id and user_name:
            cur.execute(
                """
                SELECT id, nickname
                FROM users
                WHERE nickname = %s OR email = %s
                LIMIT 1
                """,
                (user_name, user_name)
            )
            row = cur.fetchone()
            if row:
                # row is a dict in your environment
                user_id   = row["id"]
                user_name = row["nickname"]

        if not user_id:
            cur.close()
            conn.close()
            return jsonify(empty_payload(user_name or "Unknown"))

        # sum quoting metrics
        cur.execute(
            """
            SELECT
                COALESCE(SUM(quoted_items), 0)        AS quoted_items,
                COALESCE(SUM(quotes_unique), 0)       AS quotes_unique,
                COALESCE(SUM(advisor_pro_minutes), 0) AS advisor_pro_minutes
            FROM fact_daily
            WHERE user_id = %s
              AND date BETWEEN %s AND %s
            """,
            (user_id, start, end)
        )
        sums = cur.fetchone()
        cur.close()
        conn.close()

        if not sums:
            return jsonify(empty_payload(user_name or f"User #{user_id}"))

        return jsonify({
            "user_id": user_id,
            "user_name": user_name or f"User #{user_id}",
            "quoted_items": int(sums["quoted_items"] or 0),
            "quotes_unique": int(sums["quotes_unique"] or 0),
            "advisor_pro_minutes": int(sums["advisor_pro_minutes"] or 0),
            "start": start_str,
            "end": end_str
        })

    except Exception as e:
        import traceback
        print("[quoting-detail ERROR]", e, flush=True)
        traceback.print_exc()
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        return jsonify(empty_payload(user_name or "Unknown")), 200


# ---------- /api/buckets/movement-detail ----------
@app.route("/api/buckets/movement-detail", methods=["GET"])
def bucket_movement_detail():
    user_id   = request.args.get("user_id", type=int)
    user_name = request.args.get("user_name")
    start_str = request.args.get("start")
    end_str   = request.args.get("end")

    print("[movement-detail] user_id:", user_id, "user_name:", user_name,
          "start:", start_str, "end:", end_str, flush=True)

    if not (start_str and end_str):
        return jsonify({"error": "Missing date(s)"}), 400

    try:
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end   = datetime.strptime(end_str, "%Y-%m-%d").date()
    except Exception:
        return jsonify({"error": "Invalid date format"}), 400

    conn = get_db_connection()
    cur  = conn.cursor()
    print("✅ Database Connection Established", flush=True)

    def empty_payload(label):
        return {
            "user_id": user_id,
            "user_name": label,
            "keystrokes": 0,
            "mouse_clicks": 0,
            "mouse_distance": 0,
            "idle_time_seconds": 0,
            "start": start_str,
            "end": end_str
        }

    try:
        # resolve name -> id
        if not user_id and user_name:
            cur.execute(
                """
                SELECT id, nickname
                FROM users
                WHERE nickname = %s OR email = %s
                LIMIT 1
                """,
                (user_name, user_name)
            )
            row = cur.fetchone()
            if row:
                user_id   = row["id"]
                user_name = row["nickname"]

        if not user_id:
            cur.close()
            conn.close()
            return jsonify(empty_payload(user_name or "Unknown"))

        # sum movement-ish stuff
        cur.execute(
            """
            SELECT
                COALESCE(SUM(keystrokes), 0)        AS keystrokes,
                COALESCE(SUM(mouse_clicks), 0)      AS mouse_clicks,
                COALESCE(SUM(mouse_distance), 0)    AS mouse_distance,
                COALESCE(SUM(idle_time_seconds), 0) AS idle_time_seconds
            FROM fact_daily
            WHERE user_id = %s
              AND date BETWEEN %s AND %s
            """,
            (user_id, start, end)
        )
        sums = cur.fetchone()
        cur.close()
        conn.close()

        if not sums:
            return jsonify(empty_payload(user_name or f"User #{user_id}"))

        return jsonify({
            "user_id": user_id,
            "user_name": user_name or f"User #{user_id}",
            "keystrokes": int(sums["keystrokes"] or 0),
            "mouse_clicks": int(sums["mouse_clicks"] or 0),
            "mouse_distance": float(sums["mouse_distance"] or 0),
            "idle_time_seconds": int(sums["idle_time_seconds"] or 0),
            "start": start_str,
            "end": end_str
        })

    except Exception as e:
        import traceback
        print("[movement-detail ERROR]", e, flush=True)
        traceback.print_exc()
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        return jsonify(empty_payload(user_name or "Unknown")), 200

from datetime import datetime

@app.route("/api/analytics/employee-phone-series")
@login_required
def api_employee_phone_series():
    conn = get_db_connection()
    cursor = conn.cursor()

    # 🔐 Resolve manager_id safely (works even if User has no .role)
    manager_id = getattr(current_user, "manager_id", None) or current_user.id


    user_id = request.args.get("user_id", type=int)
    start = request.args.get("start")
    end = request.args.get("end")

    if not user_id or not start or not end:
        return jsonify({"error": "Missing user_id/start/end"}), 400

    # ✅ Optional safety: make sure this user belongs to this manager
    cursor.execute("""
        SELECT 1
        FROM users
        WHERE id = %s
          AND (id = %s OR manager_id = %s)
        LIMIT 1
    """, (user_id, manager_id, manager_id))

    ok = cursor.fetchone()
    if not ok:
        return jsonify({"error": "Not allowed"}), 403

    # ==========================================================
    # ✅ EDIT THESE 4 COLUMN NAMES IF YOURS ARE DIFFERENT
    # ==========================================================
    INBOUNDS_COL = "inbounds"
    OUTBOUNDS_COL = "outbounds"
    IB_TALK_MINUTES_COL = "ib_time_minutes"
    OB_TALK_MINUTES_COL = "ob_time_minutes"


    cursor.execute(f"""
        SELECT
            date,
            COALESCE({INBOUNDS_COL}, 0) AS inbounds,
            COALESCE({OUTBOUNDS_COL}, 0) AS outbounds,
            COALESCE({IB_TALK_MINUTES_COL}, 0) AS ib_talk_minutes,
            COALESCE({OB_TALK_MINUTES_COL}, 0) AS ob_talk_minutes
        FROM fact_daily
        WHERE user_id = %s
          AND date BETWEEN %s AND %s
        ORDER BY date ASC
    """, (user_id, start, end))

    rows = cursor.fetchall() or []

    labels = []
    inbounds = []
    outbounds = []
    ib_talk_minutes = []
    ob_talk_minutes = []

    for r in rows:
        day = r["date"].strftime("%Y-%m-%d") if hasattr(r["date"], "strftime") else str(r["date"])

        ib  = int(r["inbounds"] or 0)
        ob  = int(r["outbounds"] or 0)
        ibm = float(r["ib_talk_minutes"] or 0)
        obm = float(r["ob_talk_minutes"] or 0)

        # ✅ Skip days where EVERYTHING is zero
        if ib == 0 and ob == 0 and ibm == 0 and obm == 0:
            continue

        labels.append(day)
        inbounds.append(ib)
        outbounds.append(ob)
        ib_talk_minutes.append(ibm)
        ob_talk_minutes.append(obm)

    return jsonify({
        "labels": labels,
        "inbounds": inbounds,
        "outbounds": outbounds,
        "ib_talk_minutes": ib_talk_minutes,
        "ob_talk_minutes": ob_talk_minutes
    })

@app.route("/api/analytics/employee-quote-series")
@login_required
def api_employee_quote_series():
    conn = get_db_connection()
    cursor = conn.cursor()

    manager_id = getattr(current_user, "manager_id", None) or current_user.id

    user_id = request.args.get("user_id", type=int)
    start = request.args.get("start")
    end = request.args.get("end")

    if not user_id or not start or not end:
        return jsonify({"error": "Missing user_id/start/end"}), 400

    cursor.execute("""
        SELECT 1
        FROM users
        WHERE id = %s
          AND (id = %s OR manager_id = %s)
        LIMIT 1
    """, (user_id, manager_id, manager_id))

    ok = cursor.fetchone()
    if not ok:
        return jsonify({"error": "Not allowed"}), 403

    # ✅ fact_daily columns (change if yours differ)
    AP_MIN_COL = "advisor_pro_minutes"
    QUOTED_ITEMS_COL = "quoted_items"
    QUOTES_UNIQUE_COL = "quotes_unique"

    cursor.execute(f"""
        SELECT
            date,
            COALESCE({AP_MIN_COL}, 0) AS advisor_pro_minutes,
            COALESCE({QUOTED_ITEMS_COL}, 0) AS quoted_items,
            COALESCE({QUOTES_UNIQUE_COL}, 0) AS quotes_unique
        FROM fact_daily
        WHERE user_id = %s
          AND date BETWEEN %s AND %s
        ORDER BY date ASC
    """, (user_id, start, end))

    rows = cursor.fetchall() or []

    labels = []
    advisor_pro_minutes = []
    quoted_items = []
    quotes_unique = []

    for r in rows:
        day = r["date"].strftime("%Y-%m-%d") if hasattr(r["date"], "strftime") else str(r["date"])

        apm = float(r["advisor_pro_minutes"] or 0)
        qi  = int(r["quoted_items"] or 0)
        qu  = int(r["quotes_unique"] or 0)

        # ✅ Skip days where all 3 are zero
        if apm == 0 and qi == 0 and qu == 0:
            continue

        labels.append(day)
        advisor_pro_minutes.append(apm)
        quoted_items.append(qi)
        quotes_unique.append(qu)

    return jsonify({
        "labels": labels,
        "advisor_pro_minutes": advisor_pro_minutes,
        "quoted_items": quoted_items,
        "quotes_unique": quotes_unique
    })
    
@app.route("/api/analytics/team-index-series")
@login_required
def api_team_index_series():
    conn = get_db_connection()
    cursor = conn.cursor()

    manager_id = getattr(current_user, "manager_id", None) or current_user.id

    start = request.args.get("start")
    end   = request.args.get("end")
    if not start or not end:
        return jsonify({"error": "Missing start/end"}), 400

    # ==========================================================
    # ✅ Source table/columns (change ONLY if yours differ)
    # ==========================================================
    TABLE = "elite_calls_master"
    DAY_COL = "day"
    ELITE_CALLS_COL = "daily_elite_calls"
    TALK_SECONDS_COL = "daily_talk_seconds"

    # Team = all users under this manager (plus manager)
    cursor.execute(f"""
        SELECT
            e.{DAY_COL} AS day,
            SUM(COALESCE(e.{ELITE_CALLS_COL}, 0)) AS total_elite_calls,
            SUM(COALESCE(e.{TALK_SECONDS_COL}, 0)) AS total_talk_seconds
        FROM {TABLE} e
        JOIN users u ON u.id = e.user_id
        WHERE (u.manager_id = %s OR u.id = %s)
          AND e.{DAY_COL} BETWEEN %s AND %s
        GROUP BY e.{DAY_COL}
        ORDER BY e.{DAY_COL} ASC
    """, (manager_id, manager_id, start, end))

    rows = cursor.fetchall() or []

    labels = []
    team_index = []

    for r in rows:
        day = r["day"].strftime("%Y-%m-%d") if hasattr(r["day"], "strftime") else str(r["day"])

        calls = float(r["total_elite_calls"] or 0)
        talk_seconds = float(r["total_talk_seconds"] or 0)

        # skip dead days
        if calls == 0 or talk_seconds == 0:
            continue

        talk_minutes = talk_seconds / 60.0
        idx = round(calls / talk_minutes, 4)  # elite calls per minute

        labels.append(day)
        team_index.append(idx)

    return jsonify({
        "labels": labels,
        "team_index": team_index
    })
    

# ✅ Notifications
@app.route('/notifications')
def notifications():
    return render_template('notifications.html')
    
# ✅ AI Insights (replaces old Timekeeping tab)
@app.route('/timekeeping')
@login_required
def ai_insights_tab():
    return render_template('ai_insights_tab.html')


# ✅ Business Metrics
@app.route('/business_metrics')
def business_metrics():
    sample_data = [
        {
            'name': 'John',
            'activity_score': 7.2,
            'idle_spikes': 5,
            'web_focus': '74%',
            'calls': 14,
            'insight': '📉 Activity dropped 19% vs avg'
        },
        {
            'name': 'Sarah',
            'activity_score': 8.4,
            'idle_spikes': 2,
            'web_focus': '89%',
            'calls': 21,
            'insight': '💡 Most active: 10–11AM'
        },
    ]
    return render_template('business_metrics.html', data=sample_data)

# Alias route so the sidebar "Uploads" link works
@app.route('/uploads')
@login_required  # use the SAME decorator(s) you have on business_metrics
def uploads():
    # Reuse the existing Business Metrics / Uploads page
    return business_metrics()
    

# ✅ Call Metrics Page (HTML)
@app.route('/call-metrics')
def call_metrics():
    return render_template('call_metrics.html')
    
# ✅ AI Assistant API
app.register_blueprint(ai_bp)

# --- Call Metrics: date-range summary ---
from datetime import datetime, timedelta
import pytz
import mysql.connector
from flask_login import login_required, current_user

def to_hms(value):
    """Normalize MySQL TIME/SEC_TO_TIME (timedelta or string) -> 'HH:MM:SS'."""
    from datetime import timedelta as _td
    if value is None:
        return "00:00:00"
    if isinstance(value, str):
        return value
    if isinstance(value, _td):
        total = int(value.total_seconds())
        h = total // 3600
        m = (total % 3600) // 60
        s = total % 60
        return f"{h:02}:{m:02}:{s:02}"
    return str(value)

def minutes_from(value):
    """Whole minutes from either timedelta or 'HH:MM:SS'."""
    from datetime import timedelta as _td
    if value is None:
        return 0
    if isinstance(value, _td):
        return int(round(value.total_seconds() / 60.0))
    if isinstance(value, str):
        try:
            h, m, s = value.split(":")
            secs = int(h) * 3600 + int(m) * 60 + int(s)
            return int(round(secs / 60.0))
        except Exception:
            return 0
    return 0

from flask import session

@app.route("/api/call-metrics/range", methods=["GET"])
@login_required
def call_metrics_range():
    """
    Query params:
      start: YYYY-MM-DD  (inclusive, Pacific calendar date)
      end:   YYYY-MM-DD  (inclusive, Pacific calendar date)
      employee_id: user id or 'all'
      debug: 1 (optional) -> returns breakdown by local_date
    """
    # 🔐 Authoritative scope: manager from the session (NOT current_user.id)
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    start = request.args.get("start")
    end   = request.args.get("end")
    employee_id = request.args.get("employee_id", "all")
    debug = request.args.get("debug") in ("1", "true", "True")

    if not start or not end:
        return jsonify({"error": "start and end are required"}), 400

    # Treat inputs as Pacific calendar dates
    try:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date   = datetime.strptime(end, "%Y-%m-%d").date()
    except Exception as e:
        return jsonify({"error": f"bad date format: {e}"}), 400

    # Optional employee filter
    emp_filter_sql = ""
    params_base: list = [mgr_id]  # 👈 manager_id first
    if employee_id != "all":
        try:
            employee_id = int(employee_id)
            emp_filter_sql = "AND cm.user_id = %s"
            params_base.append(employee_id)
        except ValueError:
            return jsonify({"error": "employee_id must be an integer or 'all'"}), 400

    # Use Pacific day derived from created_at
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
          COALESCE(SUM(inbound_calls), 0)  AS inbound_count,
          COALESCE(SUM(outbound_calls), 0) AS outbound_count,
          SEC_TO_TIME(COALESCE(SUM(TIME_TO_SEC(inbound_time)), 0))  AS inbound_duration,
          SEC_TO_TIME(COALESCE(SUM(TIME_TO_SEC(outbound_time)), 0)) AS outbound_duration
        FROM ranked
        WHERE rn = 1
          AND local_date BETWEEN %s AND %s;
    """

    # Final param order: [manager_id, (employee_id?), start_date, end_date]
    params = params_base + [start_date, end_date]

    conn = cur = None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        row = cur.fetchone() or {}

        breakdown = []
        if debug:
            bd_sql = f"""
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
                  SUM(inbound_calls)  AS inbound,
                  SUM(outbound_calls) AS outbound,
                  SEC_TO_TIME(SUM(TIME_TO_SEC(inbound_time)))  AS inbound_time,
                  SEC_TO_TIME(SUM(TIME_TO_SEC(outbound_time))) AS outbound_time
                FROM ranked
                WHERE rn = 1
                  AND local_date BETWEEN %s AND %s
                GROUP BY local_date
                ORDER BY local_date;
            """
            cur.execute(bd_sql, params)
            breakdown = cur.fetchall()
    finally:
        if cur: cur.close()
        if conn: conn.close()

    inbound_raw  = row.get("inbound_duration")
    outbound_raw = row.get("outbound_duration")

    inbound_hms  = to_hms(inbound_raw)
    outbound_hms = to_hms(outbound_raw)

    payload = {
        "inbound_count": int(row.get("inbound_count", 0)),
        "outbound_count": int(row.get("outbound_count", 0)),
        "inbound_duration": inbound_hms,
        "outbound_duration": outbound_hms,
        "inbound_minutes": minutes_from(inbound_raw),
        "outbound_minutes": minutes_from(outbound_raw),
    }
    if debug:
        payload["by_date"] = breakdown

    return jsonify(payload)
    
def resolve_manager_id(cursor):
    """
    If I'm a manager -> manager_id = my user id.
    If I'm a user -> manager_id = users.manager_id.
    """
    cursor.execute("SELECT role, manager_id FROM users WHERE id = %s", (current_user.id,))
    me = cursor.fetchone()
    if not me:
        return None
    return current_user.id if me["role"] == "manager" else me["manager_id"]
    
def get_dict_cursor(conn):
    """
    Works for PyMySQL connections (Railway typical).
    Returns a cursor that gives dict rows.
    """
    import pymysql
    return conn.cursor(pymysql.cursors.DictCursor)
    
    

# ✅ Reports Page (Filtered by Manager)
@app.route('/reports')
@login_required
def reports():
    selected_date_str = request.args.get("date")
    page = int(request.args.get("page", 1))
    per_page = 25
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cursor = get_dict_cursor(conn)

    manager_id = resolve_manager_id(cursor)
    if not manager_id:
        cursor.close(); conn.close()
        return abort(403)

    params = [manager_id]
    where = "WHERE manager_id = %s"

    if selected_date_str:
        try:
            # ✅ User picked a *Pacific* calendar date
            selected_date = datetime.strptime(selected_date_str, "%Y-%m-%d").date()

            # ✅ Convert that Pacific day into a UTC window: [start_utc, next_day_utc)
            from datetime import time, timedelta
            import pytz

            pac = pytz.timezone("US/Pacific")
            start_pac = pac.localize(datetime.combine(selected_date, time.min))
            next_pac  = start_pac + timedelta(days=1)

            start_utc = start_pac.astimezone(pytz.utc).replace(tzinfo=None)
            next_utc  = next_pac.astimezone(pytz.utc).replace(tzinfo=None)

            where += " AND created_at >= %s AND created_at < %s"
            params.extend([start_utc, next_utc])

        except ValueError:
            selected_date_str = None  # ignore bad input


    # ✅ total rows
    cursor.execute(f"SELECT COUNT(*) AS cnt FROM reports {where}", tuple(params))
    total = int(cursor.fetchone()["cnt"] or 0)
    total_pages = max(1, (total + per_page - 1) // per_page)

    # ✅ paged rows (fast)
    cursor.execute(f"""
        SELECT id, filename, created_at
        FROM reports
        {where}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, tuple(params + [per_page, offset]))
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template(
        "reports.html",
        reports=rows,
        page=page,
        total_pages=total_pages,
        selected_date=selected_date_str
    )

# ✅ Download individual report from DB by ID
@app.route('/reports/download/<int:report_id>')
@login_required
def download_report(report_id):
    conn = get_db_connection()
    cursor = get_dict_cursor(conn)

    manager_id = resolve_manager_id(cursor)
    if not manager_id:
        cursor.close(); conn.close()
        return abort(403)

    cursor.execute("""
        SELECT filename, file_data
        FROM reports
        WHERE id = %s AND manager_id = %s
    """, (report_id, manager_id))
    row = cursor.fetchone()

    cursor.close()
    conn.close()

    if not row:
        return abort(404, "Report not found")

    return send_file(
        BytesIO(row["file_data"]),
        download_name=row["filename"],
        as_attachment=True
    )

# ✅ Generate a new report tied to manager
@app.route('/generate-report', methods=['POST'])
@login_required
def generate_report_now():
    """
    Generate report and store it in `reports` table.
    """
    import traceback
    from datetime import datetime

    conn = get_db_connection()
    cursor = get_dict_cursor(conn)

    manager_id = resolve_manager_id(cursor)
    if not manager_id:
        cursor.close(); conn.close()
        return abort(403)

    try:
        # ✅ EXPECTATION: generator returns (filename, pdf_bytes)
        from generate_daily_report import main as generate_report
        filename, pdf_bytes = generate_report(manager_id=manager_id)

        if not filename or not pdf_bytes:
            raise Exception("Generator returned empty filename or pdf bytes.")

        cursor.execute("""
            INSERT INTO reports (filename, file_data, created_at, manager_id)
            VALUES (%s, %s, %s, %s)
        """, (filename, pdf_bytes, datetime.utcnow(), manager_id))
        conn.commit()

    except Exception as e:
        conn.rollback()
        print("❌ generate-report failed:", e)
        print(traceback.format_exc())
        cursor.close(); conn.close()
        return abort(500, "Report generation failed. Check logs.")

    cursor.close()
    conn.close()
    return redirect(url_for('reports'))

# ✅ Brick Game Page
@app.route('/brick')
def brick():
    return render_template('brick.html')    

from flask import session
from flask_login import login_required

# ✅ API: Get Employees for Manager (Dropdown) — SESSION-SCOPED
@app.route("/manager-employees", methods=["GET"])
@login_required
def get_manager_employees():
    from flask import session, jsonify
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    db = get_db_connection()
    try:
        cur = dict_cursor(db)
        cur.execute(
            """
            SELECT
              id,
              email,
              nickname
            FROM users
            WHERE manager_id = %s
              AND role = 'user'
            ORDER BY email
            """,
            (mgr_id,),
        )
        employees = cur.fetchall()
        return jsonify(employees)
    except Exception as err:
        print(f"Database error in /manager-employees: {err}")
        return jsonify({"error": "Database query failed"}), 500
    finally:
        try: cur.close()
        except Exception: pass
        db.close()

# ⚠️ Legacy wrapper keeps old callers working (ignores URL id)
@app.route("/manager-employees/<int:_manager_id>", methods=["GET"])
@login_required
def get_manager_employees_legacy(_manager_id):
    return get_manager_employees()   

# ✅ Format time for idle count
def format_seconds_to_time(seconds):
    """Convert seconds to HH:MM:SS format"""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02}:{minutes:02}:{secs:02}"

# ✅ Register Web Logs API Blueprint
app.register_blueprint(weblogs_bp)

# ✅ Set Up Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

# ✅ Database Connection Function with Debugging
import os
import pymysql

def get_db_connection():
    try:
        # 🚀 Use Railway's provided DATABASE_URL
        database_url = os.getenv("RAILWAY_DATABASE_URL")

        if database_url:
            print(f"🔍 Using DATABASE_URL from Railway: {database_url}")  # Debugging
            db = pymysql.connect(
                host="mysql.railway.internal",  # Keep the internal host
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
        else:
            print("❌ RAILWAY_DATABASE_URL not found. Falling back to hardcoded config.")
            db = pymysql.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )

        print("✅ Database Connection Established")

        with db.cursor() as cursor:
            cursor.execute("SELECT DATABASE();")
            active_db = cursor.fetchone()
            print(f"📌 Flask is connected to database: {active_db['DATABASE()']}")

        return db
    except pymysql.MySQLError as e:
        print(f"❌ Database Connection Error: {e}")
        return None

# --- Dict cursor helper (works with mysql.connector or PyMySQL) ---
def dict_cursor(conn):
    try:
        return conn.cursor(dictionary=True)          # mysql.connector
    except TypeError:
        import pymysql
        return conn.cursor(pymysql.cursors.DictCursor)  # PyMySQL

@app.route("/api/user/email-reminder", methods=["GET"])
def api_get_email_reminder():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute(
        "SELECT email_login_reminder_enabled FROM users WHERE id = %s",
        (user_id,)
    )
    row = cur.fetchone()

    cur.close()
    conn.close()

    enabled = bool(row and row.get("email_login_reminder_enabled") == 1)
    return jsonify({"enabled": enabled})


@app.route("/api/user/email-reminder", methods=["POST"])
def api_set_email_reminder():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(force=True) or {}
    enabled = 1 if data.get("enabled") else 0

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "UPDATE users SET email_login_reminder_enabled = %s WHERE id = %s",
        (enabled, user_id)
    )
    conn.commit()

    cur.close()
    conn.close()

    return jsonify({"ok": True})
    
@app.route("/api/manager/users-email-reminders", methods=["GET"])
def api_manager_get_users_email_reminders():
    manager_id = session.get("manager_id")
    if not manager_id:
        return jsonify({"error": "unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id,
               email,
               COALESCE(nickname, nb_detail_name, quote_audit_name, email) AS display_name,
               email_login_reminder_enabled
        FROM users
        WHERE manager_id = %s
        ORDER BY display_name ASC
    """, (manager_id,))

    rows_raw = cur.fetchall()

    # ✅ IMPORTANT:
    # Some connectors/cursors return rows as DICTS already.
    # If we re-map them, we accidentally turn values into column names.
    if rows_raw and isinstance(rows_raw[0], dict):
        rows = rows_raw
    else:
        col_names = [desc[0] for desc in cur.description]
        rows = [dict(zip(col_names, r)) for r in rows_raw]

    cur.close()
    conn.close()

    return jsonify({"users": rows})


@app.route("/api/manager/users-email-reminders", methods=["POST"])
def api_manager_set_user_email_reminder():
    manager_id = session.get("manager_id")
    if not manager_id:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}

    # ✅ accept multiple key names just in case
    target_user_id = data.get("user_id") or data.get("id") or data.get("target_user_id")
    enabled = 1 if data.get("enabled") else 0

    if not target_user_id:
        return jsonify({"error": "missing_user_id", "received_json": data}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    # ✅ only allow updating users that belong to THIS manager
    cur.execute(
        "SELECT id FROM users WHERE id = %s AND manager_id = %s",
        (target_user_id, manager_id)
    )
    owned = cur.fetchone()
    if not owned:
        cur.close()
        conn.close()
        return jsonify({"error": "forbidden"}), 403

    cur.execute(
        "UPDATE users SET email_login_reminder_enabled = %s WHERE id = %s",
        (enabled, target_user_id)
    )
    conn.commit()

    cur.close()
    conn.close()

    return jsonify({"ok": True, "user_id": int(target_user_id), "enabled": bool(enabled)})
    

# ✅ User Model for Flask-Login
class User(UserMixin):
    def __init__(self, id, email):
        self.id = id
        self.email = email

# ✅ Flask-Login User Loader
@login_manager.user_loader
def load_user(user_id):
    db = get_db_connection()
    with db.cursor() as cursor:
        cursor.execute("SELECT id, email FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
    db.close()
    return User(user["id"], user["email"]) if user else None

# ✅ User Login Route (role + manager_id scoping)
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")

    email = request.form.get("email", "").strip()
    password = request.form.get("password", "")

    db = get_db_connection()
    try:
        cur = dict_cursor(db)
        cur.execute(
            "SELECT id, email, password_hash, role, manager_id FROM users WHERE email=%s LIMIT 1",
            (email,)
        )
        user = cur.fetchone()
    finally:
        try: cur.close()
        except Exception: pass
        db.close()

    if not user or user["password_hash"] != password:  # TODO: swap to real password check later
        return render_template("login.html", error="Invalid credentials"), 400

    # 🔐 Authoritative session scope
    session.clear()
    session.permanent = True
    session["user_id"] = user["id"]
    session["role"] = user["role"]
    session["manager_id"] = user["id"] if user["role"] == "manager" else user["manager_id"]

    # (optional) Flask-Login if you use it
    try:
        user_obj = User(user["id"], user["email"])
        login_user(user_obj)
    except Exception:
        pass

    # Redirect as before
    return redirect(url_for("dashboard_new"))

# ✅ User Logout Route
@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

# ✅ General Logs Route (with Pagination)
@app.route("/general_logs")
@login_required
def general_logs():
    per_page = 20  # Show 20 logs per page
    page = request.args.get("page", 1, type=int)  # Get the current page number
    offset = (page - 1) * per_page  # Calculate offset

    db = get_db_connection()
    with db.cursor() as cursor:
        # Get total log count for pagination
        cursor.execute("SELECT COUNT(*) as total FROM activity_log WHERE user_id = %s", (current_user.id,))
        total_logs = cursor.fetchone()["total"]
        total_pages = max((total_logs + per_page - 1) // per_page, 1)

        # Fetch logs for the current page
        cursor.execute("""
            SELECT * FROM activity_log 
            WHERE user_id = %s 
            ORDER BY timestamp DESC 
            LIMIT %s OFFSET %s
        """, (current_user.id, per_page, offset))
        logs = cursor.fetchall()
    db.close()

    # Convert JSON strings to Python dictionaries for page_time and page_percentage
    import json
    for log in logs:
        if log.get("page_time"):
            try:
                log["page_time"] = json.loads(log["page_time"])
            except Exception as e:
                log["page_time"] = {}
        else:
            log["page_time"] = {}
        if log.get("page_percentage"):
            try:
                log["page_percentage"] = json.loads(log["page_percentage"])
            except Exception as e:
                log["page_percentage"] = {}
        else:
            log["page_percentage"] = {}

    # Convert UTC timestamps to local timezone
    local_tz = pytz.timezone("America/Los_Angeles")
    for log in logs:
        try:
            utc_timestamp = log["timestamp"].replace(tzinfo=pytz.utc)
            log["timestamp"] = utc_timestamp.astimezone(local_tz).strftime("%Y-%m-%d %I:%M %p")
        except Exception as e:
            print(f"Timestamp conversion error: {e}")
            log["timestamp"] = "Unknown"

    return render_template(
        "general_logs.html",
        logs=logs,
        email=current_user.email,
        page=page,
        total_pages=total_pages
    )

# ✅ Helper Function: Convert Seconds to MM:SS Format
def format_time(seconds):
    """Convert seconds into MM:SS format."""
    minutes = seconds // 60
    sec = seconds % 60
    return f"{minutes}:{sec:02d}"  # Ensures two-digit seconds

# --- Calendar-driven Web Usage API (LA→UTC boundaries) ---
from datetime import datetime, date, time as dtime
import json, pytz
from flask import request, jsonify

@app.route("/api/web-usage", methods=["GET"])
@login_required
def api_web_usage():
    """
    Query params (Pacific dates):
      start=YYYY-MM-DD, end=YYYY-MM-DD (inclusive, Pacific)
      employee=<user_id or 'all'>
      manager_id=<manager id when employee='all'>
      limit=<int> default 10
    Returns: { data: [{label, percent}], total_seconds }
    """
    start = request.args.get("start")
    end = request.args.get("end")
    employee = request.args.get("employee", "all")
    manager_id = request.args.get("manager_id")
    try:
        limit = int(request.args.get("limit", 10))
    except Exception:
        limit = 10

    pac = pytz.timezone("America/Los_Angeles")
    utc = pytz.utc

    # Default to *Pacific* today
    la_today = datetime.now(pac).date()
    if not start: start = la_today.isoformat()
    if not end:   end   = la_today.isoformat()

    # Convert inclusive Pacific day -> UTC bounds
    s_local = pac.localize(datetime.combine(date.fromisoformat(start), dtime.min))
    e_local = pac.localize(datetime.combine(date.fromisoformat(end),   dtime.max).replace(microsecond=0))
    start_utc = s_local.astimezone(utc)
    end_utc   = e_local.astimezone(utc)

    db = get_db_connection()
    try:
        # Build WHERE for selected employee(s)
        where_user = ""
        params = [start_utc, end_utc]

        if employee and employee != "all":
            where_user = "AND al.user_id = %s"
            params.append(employee)  # expecting a numeric user_id (e.g., 8)
        else:
            target_manager_id = manager_id or str(current_user.id)
            where_user = "AND al.user_id IN (SELECT id FROM users WHERE manager_id = %s)"
            params.append(target_manager_id)

        sql = f"""
            SELECT al.page_time
            FROM activity_log al
            WHERE al.timestamp BETWEEN %s AND %s
              AND al.page_time IS NOT NULL
              AND JSON_LENGTH(al.page_time) > 0
            {where_user}
            ORDER BY al.timestamp DESC
        """

        # (optional) debug once to verify bounds
        # print(f"/api/web-usage emp={employee} start_utc={start_utc} end_utc={end_utc}")

        with db.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        # Aggregate JSON -> percentages
        by_label_seconds = {}
        total_seconds = 0.0
        for r in rows:
            raw = r["page_time"] if isinstance(r, dict) else r[0]
            if not raw: continue
            try:
                d = raw if isinstance(raw, dict) else json.loads(raw)
            except Exception:
                continue
            if not isinstance(d, dict) or not d: continue
            for label, secs in d.items():
                try:
                    s = float(secs) if secs is not None else 0.0
                except Exception:
                    s = 0.0
                if s <= 0: continue
                by_label_seconds[label] = by_label_seconds.get(label, 0.0) + s
                total_seconds += s

        denom = total_seconds if total_seconds > 0 else 1.0
        items = [{
            "label": lbl,
            "percent": round((secs / denom) * 100.0, 2),
            "seconds": int(secs)  # ← add raw seconds for tooltip
        } for lbl, secs in by_label_seconds.items()]
        items.sort(key=lambda x: x["percent"], reverse=True)
        items = items[:limit]

        return jsonify({"data": items, "total_seconds": int(total_seconds)})
        
    finally:
        try: db.close()
        except Exception: pass

# ✅ Web Logs
@app.route("/weblogs")
@login_required
def weblogs():
    per_page = 20
    page = request.args.get("page", 1, type=int)
    offset = (page - 1) * per_page

    db = get_db_connection()
    with db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) as total FROM activity_log WHERE user_id = %s", (current_user.id,))
        total_logs = cursor.fetchone()["total"]
        total_pages = max((total_logs + per_page - 1) // per_page, 1)

        cursor.execute("""
            SELECT * FROM activity_log 
            WHERE user_id = %s 
            ORDER BY timestamp DESC 
            LIMIT %s OFFSET %s
        """, (current_user.id, per_page, offset))
        logs = cursor.fetchall()
    db.close()

    import json
    for log in logs:
        # ✅ Convert page_time from seconds to MM:SS format
        if log.get("page_time"):
            try:
                log["page_time"] = json.loads(log["page_time"])
                log["page_time"] = {key: format_time(value) for key, value in log["page_time"].items()}
            except Exception as e:
                log["page_time"] = {}
        else:
            log["page_time"] = {}

        # ✅ Parse page_percentage
        if log.get("page_percentage"):
            try:
                log["page_percentage"] = json.loads(log["page_percentage"])
            except Exception as e:
                log["page_percentage"] = {}
        else:
            log["page_percentage"] = {}

    # ✅ Convert UTC timestamps to local timezone
    local_tz = pytz.timezone("America/Los_Angeles")
    for log in logs:
        try:
            utc_timestamp = log["timestamp"].replace(tzinfo=pytz.utc)
            log["timestamp"] = utc_timestamp.astimezone(local_tz).strftime("%Y-%m-%d %I:%M %p")
        except Exception as e:
            print(f"Timestamp conversion error: {e}")
            log["timestamp"] = "Unknown"

    return render_template(
        "weblogs.html",
        logs=logs,
        email=current_user.email,
        page=page,
        total_pages=total_pages
    )

# ✅ CSV Export Route
@app.route("/export_csv")
@login_required
def export_csv():
    """Generates and downloads a CSV file of the user's activity logs."""
    db = get_db_connection()
    with db.cursor() as cursor:
        cursor.execute("""
            SELECT timestamp, user_id, mouse_distance, keystrokes, mouse_clicks, idle_count 
            FROM activity_log 
            WHERE user_id = %s 
            ORDER BY timestamp DESC
        """, (current_user.id,))
        logs = cursor.fetchall()
    db.close()

    # Generate CSV content
    def generate():
        yield "Timestamp,User ID,Mouse Distance,Keystrokes,Mouse Clicks,Idle Count\n"
        for log in logs:
            yield f"{log['timestamp']},{log['user_id']},{log['mouse_distance']},{log['keystrokes']},{log['mouse_clicks']},{log['idle_count']}\n"

    response = Response(generate(), mimetype="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=activity_logs.csv"
    return response

# ✅ CSV Export Route for Web Logs (Includes Time & Percentage)
@app.route("/export_weblogs_csv")
@login_required
def export_weblogs_csv():
    """Generates and downloads a CSV file of the user's web logs usage."""
    db = get_db_connection()
    with db.cursor() as cursor:
        cursor.execute("""
            SELECT timestamp, user_id, 
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.Gateway')) AS Gateway_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.Gateway')) AS Gateway_percentage,
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.AdvisorPro')) AS AdvisorPro_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.AdvisorPro')) AS AdvisorPro_percentage,
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.Eagent')) AS Eagent_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.Eagent')) AS Eagent_percentage,
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.Outlook')) AS Outlook_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.Outlook')) AS Outlook_percentage,
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.PolicyView')) AS PolicyView_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.PolicyView')) AS PolicyView_percentage,
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.Bamboo Insurance')) AS Bamboo_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.Bamboo Insurance')) AS Bamboo_percentage,
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.Aegis Insurance')) AS Aegis_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.Aegis Insurance')) AS Aegis_percentage,
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.California Fair Plan')) AS FairPlan_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.California Fair Plan')) AS FairPlan_percentage,
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.RingCentral')) AS RingCentral_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.RingCentral')) AS RingCentral_percentage,
                   JSON_UNQUOTE(JSON_EXTRACT(page_time, '$.Other')) AS Other_time,
                   JSON_UNQUOTE(JSON_EXTRACT(page_percentage, '$.Other')) AS Other_percentage
            FROM activity_log 
            WHERE user_id = %s 
            ORDER BY timestamp DESC
        """, (current_user.id,))
        logs = cursor.fetchall()
    db.close()

    def format_time(seconds):
        try:
            seconds = int(float(seconds)) if seconds else 0
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            secs = seconds % 60
            return f"{hours:02}:{minutes:02}:{secs:02}"
        except (ValueError, TypeError):
            return "00:00:00"

    def generate():
        # ✅ Updated CSV Header
        yield "Timestamp,Gateway TT,Gateway %,AdvisorPro TT,AdvisorPro %,Eagent TT,Eagent %,Outlook TT,Outlook %,PolicyView TT,PolicyView %,Bamboo TT,Bamboo %,Aegis TT,Aegis %,FairPlan TT,FairPlan %,RingCentral TT,RingCentral %,Other TT,Other %\n"
        for log in logs:
            yield f"{log['timestamp']}," \
                  f"{format_time(log['Gateway_time'])},{log['Gateway_percentage'] or '0%'}," \
                  f"{format_time(log['AdvisorPro_time'])},{log['AdvisorPro_percentage'] or '0%'}," \
                  f"{format_time(log['Eagent_time'])},{log['Eagent_percentage'] or '0%'}," \
                  f"{format_time(log['Outlook_time'])},{log['Outlook_percentage'] or '0%'}," \
                  f"{format_time(log['PolicyView_time'])},{log['PolicyView_percentage'] or '0%'}," \
                  f"{format_time(log['Bamboo_time'])},{log['Bamboo_percentage'] or '0%'}," \
                  f"{format_time(log['Aegis_time'])},{log['Aegis_percentage'] or '0%'}," \
                  f"{format_time(log['FairPlan_time'])},{log['FairPlan_percentage'] or '0%'}," \
                  f"{format_time(log['RingCentral_time'])},{log['RingCentral_percentage'] or '0%'}," \
                  f"{format_time(log['Other_time'])},{log['Other_percentage'] or '0%'}\n"

    response = Response(generate(), mimetype="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=weblogs.csv"
    return response

# ✅ Database Connection Function (Keep this part)
def get_db_connection():
    try:
        db = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        print("✅ Database Connection Established")
        return db
    except pymysql.MySQLError as e:
        print(f"❌ Database Connection Error: {e}")
        return None

# 🚨 ADD THIS FUNCTION RIGHT BELOW THE DATABASE CONNECTION FUNCTION
@app.route("/reset_table", methods=["POST"])
def reset_table():
    """Drops and recreates the activity_log table."""
    db = get_db_connection()
    if not db:
        return jsonify({"error": "Database connection failed"}), 500

    with db.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS activity_log;")
        cursor.execute("""
            CREATE TABLE activity_log (
                id INT NOT NULL AUTO_INCREMENT,
                user_id INT NOT NULL,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                mouse_distance FLOAT DEFAULT NULL,
                keystrokes INT DEFAULT NULL,
                mouse_clicks INT DEFAULT NULL,
                idle_count FLOAT DEFAULT NULL,
                page_time JSON DEFAULT NULL,
                page_percentage JSON DEFAULT NULL,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """)
        db.commit()
    
    db.close()
    return jsonify({"message": "✅ activity_log table reset successfully"}), 200

# ✅ API: Authenticate User for tracker_script.py
@app.route("/api/authenticate", methods=["POST"])
def authenticate():
    """Authenticates the user and returns their User ID."""
    print("🚀 API HIT: /api/authenticate")  # Debugging
   
    data = request.json
    if not data:
        print("❌ ERROR: No JSON data received")
        return jsonify({"error": "No data received"}), 400

    email = data.get("email")
    password = data.get("password")

    print(f"📩 Received Login Request - Email: {email}")  # Debugging

    if not email or not password:
        print("❌ ERROR: Missing email or password")
        return jsonify({"error": "Missing email or password"}), 400

    db = get_db_connection()
    with db.cursor() as cursor:
        cursor.execute("SELECT id, password_hash FROM users WHERE email = %s", (email,))
        user = cursor.fetchone()
    db.close()

    if not user:
        print("❌ ERROR: User not found")
        return jsonify({"error": "Invalid credentials"}), 401

    print(f"✅ User Found - ID: {user['id']}")  # Debugging

    # Check if password matches
    if user["password_hash"] == password:
        print(f"🎉 Login Success - ID: {user['id']}")
        return jsonify({"user_id": user["id"]}), 200
    else:
        print("❌ ERROR: Incorrect password")
        return jsonify({"error": "Invalid credentials"}), 401

# ✅ API: Receive Activity Data from tracker_script.py
@app.route("/log_activity", methods=["POST"])
def log_activity():
    """Receives and stores user activity data."""
    data = request.json
    print(f"📩 Incoming Data: {data}")  # DEBUG LOG
    
    user_id = data.get("user_id")
    timestamp = data.get("timestamp")
    mouse_distance = data.get("mouse_distance")
    keystrokes = data.get("keystrokes")
    mouse_clicks = data.get("mouse_clicks")
    idle_count = data.get("idle_count")
    page_time = data.get("page_time")  
    page_percentage = data.get("page_percentage")

    if not user_id:
        return jsonify({"error": "User ID is required"}), 400

    try:
        utc_timestamp = datetime.fromisoformat(timestamp).astimezone(pytz.utc)
    except Exception as e:
        return jsonify({"error": f"Invalid timestamp format: {e}"}), 400

    print(f"✅ Ready to Insert: {user_id}, {utc_timestamp}, {page_time}, {page_percentage}")  # DEBUG

    db = get_db_connection()
    with db.cursor() as cursor:
        # 🚨 DEBUG: Print the table schema as Flask sees it
        cursor.execute("SHOW CREATE TABLE activity_log;")
        table_schema = cursor.fetchone()
        print(f"📌 DB Schema Seen by Flask: {table_schema}")

        # 🚨 Double-checking if 'page_time' and 'page_percentage' exist
        cursor.execute("SHOW COLUMNS FROM activity_log;")
        columns = [row["Field"] for row in cursor.fetchall()]
        print(f"🧐 Columns in activity_log: {columns}")

        if "page_time" not in columns or "page_percentage" not in columns:
            print("❌ ERROR: 'page_time' or 'page_percentage' not found in database. Aborting insert.")
            return jsonify({"error": "Database schema mismatch"}), 500

        # ✅ Insert data
        cursor.execute("""
            INSERT INTO activity_log 
            (user_id, timestamp, mouse_distance, keystrokes, mouse_clicks, idle_count, page_time, page_percentage) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            user_id, 
            utc_timestamp, 
            mouse_distance, 
            keystrokes, 
            mouse_clicks, 
            idle_count, 
            json.dumps(page_time),  # Convert to JSON string
            json.dumps(page_percentage)
        ))
        db.commit()
    db.close()

    return jsonify({"message": "Activity logged successfully"}), 200

# ✅ Homepage Route
@app.route("/")
def home():
    return """
    <html>
    <head>
        <title>Welcome to Reflexx</title>
        <style>
            body {
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                flex-direction: column;
                text-align: center;
                font-family: Arial, sans-serif;
                background-color: black;
                color: white;
            }
            img {
                width: 100px;  /* Adjusted icon size */
                height: auto;
                margin-bottom: 15px;
            }
            a {
                text-decoration: none;
                margin: 10px;
                font-size: 18px;
                color: white;
            }
            a:hover {
                color: #ffcc00;  /* Yellow on hover */
            }
        </style>
    </head>
    <body>
        <img src="/static/reflexx_logo.png" alt="Reflexx Logo">  <!-- Smaller logo -->
        <h1>Welcome to Reflexx!</h1>
        <p>
            <a href='/login'>🔑 Login</a> | 
        </p>
    </body>
    </html>
    """
    
from flask import send_file

@app.route('/download_tracker')
def download_tracker():
    return send_file("ReflexxApp 2.1.exe", as_attachment=True, download_name="ReflexxApp 2.1.exe", mimetype="application/octet-stream")

# ✅ Dashboard Route with Web Logs Chart
@app.route("/dashboard")
@login_required  # Keep this if login is required
def dashboard():
    print(f"📌 Debug: Session Data - {dict(session)}")  # ✅ Debug session contents
    user_id = session.get("user_id")  # ✅ Retrieve user_id from session

    if not user_id:
        print("❌ No user_id found in session!")
        return redirect(url_for("login"))  # If session is broken, force login again
    
    # ✅ Render template with user_id
    return render_template("dashboard.html", user_id=user_id)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)

from flask import request, jsonify
import pandas as pd
import mysql.connector
from flask_login import login_required, current_user

@app.route("/api/upload-business-metrics", methods=["POST"])
@login_required
def upload_business_metrics():
    file = request.files.get("file")

    # Raw form values
    month_raw = request.form.get("month")
    year_raw = request.form.get("year")

    # ✅ Normalize month/year
    MONTH_MAP = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }

    def normalize_month(m):
        if m is None:
            return None
        m = str(m).strip()
        if m.isdigit():
            mi = int(m)
            return mi if 1 <= mi <= 12 else None
        return MONTH_MAP.get(m.lower())

    month = normalize_month(month_raw)
    year = int(year_raw) if (year_raw and str(year_raw).strip().isdigit()) else None

    user_id = current_user.id

    if not file or not month or not year:
        return jsonify({
            "error": "Missing file, month, or year",
            "received": {"month": month_raw, "year": year_raw, "has_file": bool(file)}
        }), 400

    try:
        # Read excel (no headers)
        df = pd.read_excel(file, header=None)

        def clean_value(cell):
            if cell is None:
                return None
            if isinstance(cell, str):
                cell = cell.replace("$", "").replace(",", "").replace("%", "").strip()
            try:
                return float(cell)
            except Exception:
                return None

        # ✅ L23, L40, L45 (0-indexed: row-1, col-1)
        net_ret = clean_value(df.iloc[22, 11])     # L23
        wa_prem = clean_value(df.iloc[39, 11])     # L40
        loss_ratio = clean_value(df.iloc[44, 11])  # L45

        filename = file.filename or "uploaded.xlsx"

        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # ✅ Replace the month (DO NOT depend on filename)
        cursor.execute("""
            DELETE FROM business_metrics
            WHERE user_id = %s AND month = %s AND year = %s
        """, (user_id, month, year))

        metrics = [
            ("Net Retention", net_ret),
            ("W&A Premium", wa_prem),
            ("12MM Loss Ratio", loss_ratio),
        ]

        inserted = 0
        for metric_name, value in metrics:
            if value is not None:
                cursor.execute("""
                    INSERT INTO business_metrics
                      (metric_name, value, month, year, source_file, uploaded_at, user_id)
                    VALUES
                      (%s, %s, %s, %s, %s, NOW(), %s)
                """, (metric_name, value, month, year, filename, user_id))
                inserted += 1

        conn.commit()

        # ✅ Debug proof in logs (Railway)
        cursor.execute("""
            SELECT metric_name, value, month, year, source_file, uploaded_at, user_id
            FROM business_metrics
            WHERE user_id = %s AND month = %s AND year = %s
            ORDER BY metric_name
        """, (user_id, month, year))
        rows = cursor.fetchall()
        print("✅ Business metrics saved:", rows)

        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "inserted": inserted,
            "filename": filename,
            "month": month,
            "year": year,
            "values_read": {
                "Net Retention": net_ret,
                "W&A Premium": wa_prem,
                "12MM Loss Ratio": loss_ratio
            }
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

def import_nb_detail_report(file_stream, manager_id):
    """
    Import 'New Business Details' Excel into nb_detail_reports.

    - Keeps one row per policy (with policy_no + item_count)
    - Only Standard Auto rows
    - Only rows where Disposition Code = "New Policy Issued"
    - ALWAYS replaces existing data for the date range covered
      by this file for that manager.
    """
    # 1) Read the file with NO header so we can control the header row
    df_raw = pd.read_excel(file_stream, header=None)

    # 2) Row 4 (0-based index) has the real column names like:
    #    'Agent Number', 'Sub Producer', 'Sub-Producer Name',
    #    'Issued Date', 'Product', 'Item Count', 'Policy No',
    #    'Disposition Code', etc.
    header_row = df_raw.iloc[4].tolist()

    # 3) Data starts at row 5 and below
    df = df_raw.iloc[5:].copy()
    df.columns = header_row

    # 4) Keep only the columns we care about
    needed_cols = [
        "Sub Producer",
        "Sub-Producer Name",
        "Issued Date",
        "Product",
        "Item Count",
        "Policy No",
        "Disposition Code",   # 👈 NEW: we care about this for filtering
    ]
    existing_cols = [c for c in needed_cols if c in df.columns]
    df = df[existing_cols]

    # 5) Filter: only Standard Auto rows
    if "Product" in df.columns:
        df = df[df["Product"] == "Standard Auto"]

    # 6) Filter: only "New Policy Issued" rows
    if "Disposition Code" in df.columns:
        df = df[df["Disposition Code"] == "New Policy Issued"]

    # 7) Drop empty rows (no sub producer / issue date)
    if "Sub-Producer Name" in df.columns:
        df = df[df["Sub-Producer Name"].notna()]
    if "Issued Date" in df.columns:
        df = df[df["Issued Date"].notna()]

    # 8) Normalize / convert types
    #    Convert Issued Date to real DATE objects
    if "Issued Date" in df.columns:
        df["Issued Date"] = pd.to_datetime(df["Issued Date"], errors="coerce").dt.date
        df = df[df["Issued Date"].notna()]
    else:
        # No dates -> nothing useful to import
        return 0

    #    Make sure Item Count is an int (missing -> 0)
    if "Item Count" in df.columns:
        df["Item Count"] = pd.to_numeric(df["Item Count"], errors="coerce").fillna(0).astype(int)
    else:
        df["Item Count"] = 0

    #    Ensure Sub Producer code is a string
    if "Sub Producer" in df.columns:
        df["Sub Producer"] = df["Sub Producer"].astype(str)
    else:
        df["Sub Producer"] = ""

    #    Ensure Policy No exists
    if "Policy No" not in df.columns:
        df["Policy No"] = ""

    # 9) If there is no data after filtering, bail out early
    if df.empty:
        return 0

    # 10) Figure out the date range covered by this file
    min_date = df["Issued Date"].min()
    max_date = df["Issued Date"].max()

    # 11) Connect to MySQL
    conn = get_db_connection()  # <-- your existing helper
    cursor = conn.cursor()

    # 12) DELETE old rows for this manager & date range
    delete_sql = """
        DELETE FROM nb_detail_reports
        WHERE manager_id = %s
          AND issue_date BETWEEN %s AND %s
    """
    cursor.execute(delete_sql, (manager_id, min_date, max_date))

    # 13) INSERT fresh rows
    insert_sql = """
        INSERT INTO nb_detail_reports
          (manager_id, sub_producer_code, sub_producer_name,
           issue_date, product, item_count, policy_no)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s)
    """

    rows_inserted = 0
    for _, row in df.iterrows():
        cursor.execute(
            insert_sql,
            (
                manager_id,
                row.get("Sub Producer", ""),
                row.get("Sub-Producer Name", "").strip() if row.get("Sub-Producer Name") else "",
                row.get("Issued Date"),
                row.get("Product", ""),
                int(row.get("Item Count", 0)),
                str(row.get("Policy No", "")),
            ),
        )
        rows_inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    return rows_inserted

@app.route("/upload_nb_details", methods=["POST"])
@login_required
def upload_nb_details():
    # 👇 figure out which manager this belongs to
    manager_id = getattr(current_user, "manager_id", None) or current_user.id

    file = request.files.get("file")
    if not file:
        return jsonify({"success": False, "error": "No file uploaded"}), 400

    # basic metadata about the file
    original_name = file.filename or ""
    file_size = file.content_length or 0

    # ✅ where we store NB copies
    nb_folder = os.path.join("uploads", "nb_details")
    os.makedirs(nb_folder, exist_ok=True)

    # ✅ make a unique saved filename
    base, ext = os.path.splitext(original_name)
    safe_ext = ext or ".xlsx"
    saved_name = f"nb_{int(time.time())}_{manager_id}{safe_ext}"
    saved_path = os.path.join(nb_folder, saved_name)

    try:
        # 1) save a copy to disk
        file.save(saved_path)

        # 2) import rows into nb_detail_reports from the saved file
        rows_inserted = import_nb_detail_report(saved_path, manager_id)

        # 3) log the upload itself so we can show history + allow download
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO nb_detail_uploads (manager_id, original_name, saved_name, file_size)
            VALUES (%s, %s, %s, %s)
            """,
            (manager_id, original_name, saved_name, file_size),
        )

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"success": True, "rows_inserted": rows_inserted})
    except Exception as e:
        current_app.logger.exception("NB upload failed")
        return jsonify({"success": False, "error": str(e)}), 500
        
import math  # make sure this is at the top of the file
     
@app.route("/api/nb_uploads/list")
@login_required
def api_nb_uploads_list():
    page = int(request.args.get("page", 1))
    per_page = 5
    offset = (page - 1) * per_page

    manager_id = getattr(current_user, "manager_id", None) or current_user.id

    conn = get_db_connection()
    cur = conn.cursor()

    # total count
    cur.execute("""
        SELECT COUNT(*) AS c
        FROM nb_detail_uploads
        WHERE manager_id = %s
    """, (manager_id,))
    row = cur.fetchone()
    total_rows = row["c"] if row else 0

    # page rows
    cur.execute("""
        SELECT id, original_name, file_size, uploaded_at
        FROM nb_detail_uploads
        WHERE manager_id = %s
        ORDER BY uploaded_at DESC
        LIMIT %s OFFSET %s
    """, (manager_id, per_page, offset))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "original_name": r["original_name"],
            "file_size": r["file_size"],
            "uploaded_at": r["uploaded_at"],
        })

    total_pages = max(1, (total_rows + per_page - 1) // per_page)

    return jsonify({
        "page": page,
        "per_page": per_page,
        "total_rows": total_rows,
        "total_pages": total_pages,
        "items": items
    })

@app.route("/nb_uploads/download/<int:upload_id>")
@login_required
def nb_uploads_download(upload_id):
    manager_id = getattr(current_user, "manager_id", None) or current_user.id

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT original_name, saved_name
        FROM nb_detail_uploads
        WHERE id = %s AND manager_id = %s
        """,
        (upload_id, manager_id),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return "Not found", 404

    # row is a dict from your connector
    original_name = row["original_name"]
    saved_name = row["saved_name"]

    nb_folder = os.path.join("uploads", "nb_details")
    return send_from_directory(
        nb_folder,
        saved_name,
        as_attachment=True,
        download_name=original_name,
    )

@app.route("/api/metrics-trend")
@login_required
def metrics_trend():
    user_id = session.get("user_id")
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT metric_name, value, month, year
        FROM business_metrics
        WHERE user_id = %s
        ORDER BY metric_name, year, month
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    from collections import defaultdict
    import calendar

    trends = defaultdict(lambda: {"labels": [], "values": []})

    for metric_name, value, month, year in rows:
        label = f"{calendar.month_abbr[month]} {year}"
        trends[metric_name]["labels"].append(label)
        trends[metric_name]["values"].append(float(value))

    return jsonify(trends)

@app.route("/api/get_employee_data")
def get_employee_data():
    employee_id = request.args.get("employee_id")
    time_range = request.args.get("time_range")

    # For now, just return a dummy response for testing
    return jsonify({
        "total_mouse_distance": 1234,
        "daily_avg_mouse_distance": 100,
        "total_keystrokes": 5432,
        "daily_avg_keystrokes": 500,
        "total_mouse_clicks": 987,
        "daily_avg_mouse_clicks": 90,
        "total_idle_count_formatted": "01:23:45",
        "daily_avg_idle_count": "00:12:34"
    })

# =========================
# REFLEXX FAST-CACHE BLOCK (with alias + fail-fast)
# =========================
import os, time, threading, json
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception:
    from pytz import timezone as _tz
    class ZoneInfo:
        def __init__(self, name): self._tz = _tz(name)
        def __getattr__(self, x): return getattr(self._tz, x)

import mysql.connector
from mysql.connector import pooling
from flask import jsonify, request

# ---- timezones ----
PT  = ZoneInfo("America/Los_Angeles")
UTC = ZoneInfo("UTC")

# ---- DB config ----
def _reflexx_db_config():
    host = globals().get("DB_HOST", os.getenv("DB_HOST", "mysql.railway.internal"))
    user = globals().get("DB_USER", os.getenv("DB_USER", "root"))
    pwd  = globals().get("DB_PASSWORD", os.getenv("DB_PASSWORD"))
    name = globals().get("DB_NAME", os.getenv("DB_NAME", "railway"))
    port = int(globals().get("DB_PORT", os.getenv("DB_PORT", 3306)))
    if not pwd:
        print("[reflexx-cache] WARNING: DB_PASSWORD missing; set an env var or define DB_PASSWORD earlier.")
    return dict(host=host, user=user, password=pwd, database=name, port=port)

# ---- MySQL pool ----
try:
    REFLEXX_POOL = pooling.MySQLConnectionPool(
        pool_name="reflexx_pool",
        pool_size=6,
        pool_reset_session=True,
        **_reflexx_db_config(),
    )
except Exception as e:
    print("[reflexx-cache] ERROR creating pool:", e)
    raise

def _reflexx_db():
    conn = REFLEXX_POOL.get_connection()
    try:
        conn.ping(reconnect=True, attempts=1, delay=0)
    except Exception:
        try: conn.close()
        except Exception: pass
        conn = REFLEXX_POOL.get_connection()
    return conn

# ---- in-memory cache ----
REFLEXX_CACHE = {"call_stats": {}, "web_usage": {}}
REFLEXX_CACHE_LOCK = threading.Lock()

def _reflexx_today_datestr():
    return datetime.now(PT).date().isoformat()

# ---- latest metric_date ----
def _reflexx_latest_metric_date():
    try:
        conn = _reflexx_db(); cur = conn.cursor()
        cur.execute("SELECT MAX(metric_date) FROM call_metrics_cache")
        row = cur.fetchone()
        cur.close(); conn.close()
        if row and row[0]:
            return row[0].strftime("%Y-%m-%d")
    except Exception as e:
        print("[reflexx-cache] latest_metric_date error:", e)
    return None

# ---- read call stats (fail-fast) ----
def _reflexx_load_call_stats(date_str: str):
    try:
        conn = _reflexx_db(); cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT user_email, inbounds, outbounds, inbound_seconds, outbound_seconds, updated_at
            FROM call_metrics_cache
            WHERE metric_date=%s
        """, (date_str,))
        rows = cur.fetchall()
        cur.close(); conn.close()
        last_updated = max((r["updated_at"] for r in rows), default=None)
        return {
            "date": date_str,
            "rows": rows,
            "last_updated": (last_updated.isoformat() if last_updated else None),
            "ok": True,
        }
    except Exception as e:
        print("[reflexx-cache] load_call_stats error:", e)
        return {"date": date_str, "rows": [], "last_updated": None, "ok": False}

# ---- roll up web usage ----
def _reflexx_rollup_web_usage(date_str: str):
    try:
        d = datetime.fromisoformat(date_str).replace(tzinfo=PT)
        start_pt = d.replace(hour=0, minute=0, second=0, microsecond=0)
        end_pt   = start_pt + timedelta(days=1)
        start_utc = start_pt.astimezone(UTC).replace(tzinfo=None)
        end_utc   = end_pt.astimezone(UTC).replace(tzinfo=None)

        conn = _reflexx_db(); cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT user_id, page_time
            FROM activity_log
            WHERE timestamp >= %s AND timestamp < %s
        """, (start_utc, end_utc))

        per_user = {}
        for r in cur:
            uid = r["user_id"]; pt = r["page_time"]
            if isinstance(pt, str):
                try: pt = json.loads(pt)
                except Exception: pt = {}
            if not isinstance(pt, dict): pt = {}
            dct = per_user.setdefault(uid, {})
            for label, secs in pt.items():
                try:
                    dct[label] = dct.get(label, 0) + int(secs or 0)
                except Exception: continue
        cur.close(); conn.close()

        out = []
        for uid, labels in per_user.items():
            total = sum(labels.values()) or 1
            for label, secs in labels.items():
                out.append({
                    "user_id": uid,
                    "label": label,
                    "seconds": int(secs),
                    "pct": round(100.0 * secs / total, 2)
                })
        return {"date": date_str, "rows": out, "last_updated": datetime.now(PT).isoformat(), "ok": True}
    except Exception as e:
        print("[reflexx-cache] rollup_web_usage error:", e)
        return {"date": date_str, "rows": [], "last_updated": None, "ok": False}

# ---- background refresh ----
def _reflexx_cache_loop():
    last_date_cleared = None
    while True:
        try:
            today_str  = _reflexx_today_datestr()
            latest_str = _reflexx_latest_metric_date() or today_str

            if today_str != last_date_cleared:
                with REFLEXX_CACHE_LOCK:
                    REFLEXX_CACHE["call_stats"].clear()
                    REFLEXX_CACHE["web_usage"].clear()
                last_date_cleared = today_str

            calls_today = _reflexx_load_call_stats(today_str)
            web_today   = _reflexx_rollup_web_usage(today_str)
            with REFLEXX_CACHE_LOCK:
                REFLEXX_CACHE["call_stats"][today_str] = calls_today
                REFLEXX_CACHE["web_usage"][today_str]  = web_today

            if latest_str != today_str:
                calls_latest = _reflexx_load_call_stats(latest_str)
                with REFLEXX_CACHE_LOCK:
                    REFLEXX_CACHE["call_stats"][latest_str] = calls_latest

        except Exception as e:
            print("[reflexx-cache] ERROR in cache loop:", e)
        time.sleep(75)

if not globals().get("_REFLEXX_CACHE_THREAD_STARTED", False):
    threading.Thread(target=_reflexx_cache_loop, daemon=True).start()
    _REFLEXX_CACHE_THREAD_STARTED = True
    print("[reflexx-cache] background cache thread started")

# ---- endpoints ----
from flask_login import login_required
from flask import session, jsonify, request
from datetime import datetime, date
from zoneinfo import ZoneInfo
import mysql.connector

PACIFIC = ZoneInfo("America/Los_Angeles")

def _ymd(dt: date) -> str:
    return dt.strftime("%Y-%m-%d")

def _today_pacific_datestr() -> str:
    return _ymd(datetime.now(PACIFIC).date())

@app.get("/api/call-stats")
@login_required
def call_stats_daily():
    """
    Returns manager-scoped call stats for a single Pacific calendar day.
    Query params:
      date=YYYY-MM-DD  (Pacific day) or 'latest' (find latest day for this manager)
      employee_id=<id>|all   (optional, default 'all')
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    employee_id = request.args.get("employee_id", "all")
    req_date = request.args.get("date")

    # Resolve target Pacific day (YYYY-MM-DD)
    if req_date == "latest":
        conn = mysql.connector.connect(**app.config["MYSQL_CONFIG"])
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT MAX(DATE(CONVERT_TZ(cm.created_at,'UTC','America/Los_Angeles')))
                FROM call_metrics cm
                JOIN users u ON u.id = cm.user_id
                WHERE u.manager_id = %s
            """, (mgr_id,))
            row = cur.fetchone()
            target_ymd = row[0].strftime("%Y-%m-%d") if row and row[0] else _today_pacific_datestr()
        finally:
            try: cur.close()
            except: pass
            conn.close()
    else:
        target_ymd = req_date or _today_pacific_datestr()
        try:
            datetime.strptime(target_ymd, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": "bad date format, expected YYYY-MM-DD"}), 400

    # ---- OPTIONAL: cache (TEMP DISABLED) ----
    cache_key = f"{mgr_id}:{employee_id}:{target_ymd}"
    with REFLEXX_CACHE_LOCK:
        cached = REFLEXX_CACHE["call_stats"].get(cache_key)
    # if cached is not None:
    #     return jsonify(cached)


    # ---- Query latest snapshot per user for that Pacific day ----
    emp_filter_sql = ""
    params = [mgr_id]
    emp_filter_sql_rico = ""
    params_rico = [mgr_id]

    if employee_id != "all":
        try:
            emp_id = int(employee_id)
        except ValueError:
            return jsonify({"error": "employee_id must be integer or 'all'"}), 400

        # for call_metrics
        emp_filter_sql = " AND cm.user_id = %s"
        params.append(emp_id)

        # for rico_call_metrics (note: column is reflexx_user_id, not user_id)
        emp_filter_sql_rico = " AND rcm.reflexx_user_id = %s"
        params_rico.append(emp_id)

    # date param for both queries
    params.append(target_ymd)
    params_rico.append(target_ymd)


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
              ) rn
            FROM base
        )
        SELECT
          COALESCE(SUM(inbound_calls), 0)  AS inbound_count,
          COALESCE(SUM(outbound_calls), 0) AS outbound_count,
          SEC_TO_TIME(COALESCE(SUM(TIME_TO_SEC(inbound_time)), 0))  AS inbound_duration,
          SEC_TO_TIME(COALESCE(SUM(TIME_TO_SEC(outbound_time)), 0)) AS outbound_duration
        FROM ranked
        WHERE rn = 1
          AND local_date = %s;
    """

    conn = mysql.connector.connect(**app.config["MYSQL_CONFIG"])
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, params)
        row = cur.fetchone() or {}
        rico_row = {}
        # ---- Ricochet (rico_call_metrics) aggregation for the same day ----
        sql_rico = f"""
            SELECT
              COALESCE(SUM(rcm.inbound_calls), 0)           AS rc_inbound_count,
              COALESCE(SUM(rcm.outbound_calls), 0)          AS rc_outbound_count,
              SEC_TO_TIME(COALESCE(SUM(rcm.inbound_talk_sec), 0))  AS rc_inbound_duration,
              SEC_TO_TIME(COALESCE(SUM(rcm.outbound_talk_sec), 0)) AS rc_outbound_duration
            FROM rico_call_metrics rcm
            WHERE rcm.manager_id = %s
              {emp_filter_sql_rico}
              AND rcm.day = %s;
        """

        try:
            cur.execute(sql_rico, params_rico)
            rico_row = cur.fetchone() or {}
        except Exception as e:
            # If Ricochet query fails, log it but do NOT break the whole API
            print("[/api/call-stats] Ricochet query failed:", e)
            rico_row = {}

        payload = {
            # RingCentral (call_metrics)
            "inbound_count": int(row.get("inbound_count") or 0),
            "outbound_count": int(row.get("outbound_count") or 0),
            "inbound_duration": str(row.get("inbound_duration") or "00:00:00"),
            "outbound_duration": str(row.get("outbound_duration") or "00:00:00"),

            # Ricochet (rico_call_metrics)
            "rc_inbound_count": int(rico_row.get("rc_inbound_count") or 0),
            "rc_outbound_count": int(rico_row.get("rc_outbound_count") or 0),
            "rc_inbound_duration": str(rico_row.get("rc_inbound_duration") or "00:00:00"),
            "rc_outbound_duration": str(rico_row.get("rc_outbound_duration") or "00:00:00"),

            # Day label
            "date": target_ymd,
        }

    finally:
        try: cur.close()
        except: pass
        conn.close()

    with REFLEXX_CACHE_LOCK:
        REFLEXX_CACHE["call_stats"][cache_key] = payload

    return jsonify(payload)

# =========================
# AI Dashboard Insights API
# =========================
@app.get("/api/ai-dashboard-insights")
@login_required
def ai_dashboard_insights():
    """
    Returns manager-scoped AI dashboard insights.
    Query params:
      window=overall|last_7_days|last_14_days|last_30_days (default overall)
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    window = request.args.get("window", "overall")

    conn = mysql.connector.connect(**app.config["MYSQL_CONFIG"])
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT ai.window_label,
                   ai.polarity,
                   ai.user_id,
                   u.nickname AS full_name,
                   ai.insight_type,
                   ai.raw_message,
                   ai.severity_score,
                   ai.start_date,
                   ai.end_date
            FROM ai_dashboard_insights ai
            JOIN users u ON u.id = ai.user_id
            WHERE ai.manager_id = %s
              AND ai.window_label = %s
            ORDER BY ai.polarity, ai.severity_score DESC;
        """, (mgr_id, window))

        rows = cur.fetchall()
        return jsonify({
            "window": window,
            "count": len(rows),
            "rows": rows
        })

    finally:
        cur.close()
        conn.close()

# ==========================================
# AI Insights Tab APIs (history browser)
# ==========================================
@app.get("/api/ai-insight-runs")
@login_required
def ai_insight_runs():
    """
    Returns manager-scoped list of insight runs (grouped by run_id),
    newest first.
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    conn = mysql.connector.connect(**app.config["MYSQL_CONFIG"])
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT run_id,
                   MAX(end_date) AS end_date
            FROM ai_insight_candidates
            WHERE manager_id = %s
            GROUP BY run_id
            ORDER BY end_date DESC;
        """, (mgr_id,))

        rows = cur.fetchall()

        out = []
        for r in rows:
            d = r["end_date"]
            label = d.strftime("%m-%d-%y Insights") if d else r["run_id"]
            out.append({
                "run_id": r["run_id"],
                "label": label,
                "end_date": str(d) if d else None
            })

        return jsonify(out)
    finally:
        cur.close()
        conn.close()


@app.get("/api/ai-insight-candidates")
@login_required
def ai_insight_candidates():
    """
    Returns all candidates for a given run_id + window_label.
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    run_id = request.args.get("run_id")
    window = request.args.get("window_label")

    if not run_id or not window:
        return jsonify({"error": "run_id and window_label are required"}), 400

    conn = mysql.connector.connect(**app.config["MYSQL_CONFIG"])
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT c.run_id,
                   c.window_label,
                   c.polarity,
                   c.user_id,
                   u.nickname AS full_name,
                   c.insight_type,
                   c.raw_title,
                   c.raw_message,
                   c.severity_score,
                   c.start_date,
                   c.end_date
            FROM ai_insight_candidates c
            JOIN users u ON u.id = c.user_id
            WHERE c.manager_id = %s
              AND c.run_id = %s
              AND c.window_label = %s
            ORDER BY c.polarity, c.severity_score DESC;
        """, (mgr_id, run_id, window))

        rows = cur.fetchall()
        return jsonify({"rows": rows})
    finally:
        cur.close()
        conn.close()

#Download CSV for AI Insights        
import csv
from io import StringIO
from flask import Response

@app.get("/api/ai-insight-candidates.csv")
@login_required
def ai_insight_candidates_csv():
    """
    Download CSV for a given run_id + window_label (manager-scoped).
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    run_id = request.args.get("run_id")
    window = request.args.get("window_label")

    if not run_id or not window:
        return jsonify({"error": "run_id and window_label are required"}), 400

    conn = mysql.connector.connect(**app.config["MYSQL_CONFIG"])
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT c.run_id,
                   c.window_label,
                   c.polarity,
                   c.user_id,
                   u.nickname AS full_name,
                   c.insight_type,
                   c.raw_title,
                   c.raw_message,
                   c.severity_score,
                   c.start_date,
                   c.end_date
            FROM ai_insight_candidates c
            JOIN users u ON u.id = c.user_id
            WHERE c.manager_id = %s
              AND c.run_id = %s
              AND c.window_label = %s
            ORDER BY c.polarity, c.severity_score DESC;
        """, (mgr_id, run_id, window))

        rows = cur.fetchall()

        # Build CSV in memory
        output = StringIO()
        writer = csv.writer(output)

        # Header
        writer.writerow([
            "run_id", "window_label", "polarity",
            "user_id", "full_name", "insight_type",
            "raw_title", "raw_message",
            "severity_score", "start_date", "end_date"
        ])

        # Rows
        for r in rows:
            writer.writerow([
                r.get("run_id"),
                r.get("window_label"),
                r.get("polarity"),
                r.get("user_id"),
                r.get("full_name"),
                r.get("insight_type"),
                r.get("raw_title"),
                r.get("raw_message"),
                r.get("severity_score"),
                r.get("start_date"),
                r.get("end_date"),
            ])

        csv_data = output.getvalue()
        output.close()

        filename = f"ai_insights_{run_id}_{window}.csv"

        return Response(
            csv_data,
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    finally:
        cur.close()
        conn.close()

@app.get("/api/ai-insight-run.csv")
@login_required
def ai_insight_run_csv():
    """
    Download ONE CSV containing ALL windows for a run_id (manager-scoped).
    Includes last_7_days, last_14_days, last_30_days together.
    """
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    run_id = request.args.get("run_id")
    if not run_id:
        return jsonify({"error": "run_id is required"}), 400

    conn = mysql.connector.connect(**app.config["MYSQL_CONFIG"])
    cur = conn.cursor(dictionary=True)

    try:
        cur.execute("""
            SELECT c.run_id,
                   c.window_label,
                   c.polarity,
                   c.user_id,
                   u.nickname AS full_name,
                   c.insight_type,
                   c.raw_title,
                   c.raw_message,
                   c.severity_score,
                   c.start_date,
                   c.end_date
            FROM ai_insight_candidates c
            JOIN users u ON u.id = c.user_id
            WHERE c.manager_id = %s
              AND c.run_id = %s
              AND c.window_label IN ('last_7_days','last_14_days','last_30_days')
            ORDER BY c.window_label,
                     c.polarity,
                     c.severity_score DESC;
        """, (mgr_id, run_id))

        rows = cur.fetchall()

        output = StringIO()
        writer = csv.writer(output)

        writer.writerow([
            "run_id", "window_label", "polarity",
            "user_id", "full_name", "insight_type",
            "raw_title", "raw_message",
            "severity_score", "start_date", "end_date"
        ])

        for r in rows:
            writer.writerow([
                r.get("run_id"),
                r.get("window_label"),
                r.get("polarity"),
                r.get("user_id"),
                r.get("full_name"),
                r.get("insight_type"),
                r.get("raw_title"),
                r.get("raw_message"),
                r.get("severity_score"),
                r.get("start_date"),
                r.get("end_date"),
            ])

        csv_data = output.getvalue()
        output.close()

        filename = f"ai_insights_{run_id}_ALL_WINDOWS.csv"

        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    finally:
        cur.close()
        conn.close()        


# ---- alias for legacy UI ----
@app.get("/api/call-metrics")
def reflexx_api_call_metrics_alias():
    return reflexx_api_call_stats()

# ---- web usage shape ----
def _reflexx_web_usage_legacy_shape(cached_obj):
    rows = (cached_obj or {}).get("rows", []) or []
    label_seconds = {}
    for r in rows:
        label = r.get("label") or "Other"
        secs  = int(r.get("seconds") or 0)
        label_seconds[label] = label_seconds.get(label, 0) + secs
    total = sum(label_seconds.values()) or 0
    data = []
    if total > 0:
        for label, secs in sorted(label_seconds.items(), key=lambda kv: -kv[1]):
            pct = round(secs * 100.0 / total, 2)
            data.append({"label": label, "percent": pct})
    else:
        data = [{"label": "Other", "percent": 0.0}]
    return {"data": data, "total_seconds": total, "ok": (total > 0)}

@app.get("/api/web-usage")
def reflexx_api_web_usage():
    date_str = request.args.get("date") or _reflexx_today_datestr()
    with REFLEXX_CACHE_LOCK:
        cached = REFLEXX_CACHE["web_usage"].get(date_str)
    if not cached:
        cached = _reflexx_rollup_web_usage(date_str)
        with REFLEXX_CACHE_LOCK:
            REFLEXX_CACHE["web_usage"][date_str] = cached
    return jsonify(_reflexx_web_usage_legacy_shape(cached))

@app.get("/api/ping")
def reflexx_api_ping():
    return jsonify({"ok": True})
# ===== end REFLEXX FAST-CACHE BLOCK =====


#Rico Webhook
from ricochet_webhook import ricochet_bp
app.register_blueprint(ricochet_bp)

import os; print("RICOCHET_WEBHOOK_TOKEN present?", bool(os.getenv("RICOCHET_WEBHOOK_TOKEN")))

#Adding Rico to Reflexx
# app.py
from flask_login import login_required
from flask import session, request, jsonify
from datetime import datetime

def _parse_ymd(s: str):
    return datetime.strptime(s, "%Y-%m-%d")  # raises if bad

@app.get("/api/calls/summary")
@login_required
def calls_summary():
    mgr_id = session.get("manager_id")
    if not mgr_id:
        return jsonify({"error": "unauthorized"}), 401

    # support ?fr=&end= (your frontend) and ?from=&to=
    date_from = request.args.get("from") or request.args.get("fr")
    date_to   = request.args.get("to")   or request.args.get("end") or date_from

    try:
        _parse_ymd(date_from)
        _parse_ymd(date_to)
    except Exception:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    rows = []
    ricochet_fd = {"inb": 0, "sec": 0}
    rico_exact = None
    user_id = None
    has_rico = False

    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # figure out if a single user was picked
        user_key = request.args.get("user_key", "ALL")
        if user_key and user_key != "ALL":
            resolved_by_email = False
            try:
                user_id = int(user_key)
                cur.execute(
                    "SELECT id, ricochet_user_id FROM users WHERE id=%s AND manager_id=%s",
                    (user_id, mgr_id),
                )
            except ValueError:
                resolved_by_email = True
                cur.execute(
                    """
                    SELECT id, ricochet_user_id
                    FROM users
                    WHERE (email = %s OR nickname = %s)
                      AND manager_id = %s
                    """,
                    (user_key, user_key, mgr_id),
                )

            row = cur.fetchone()
            if not row:
                return jsonify({"error": "User not found for this manager"}), 404

            # row can be a dict or a tuple; handle both safely
            if isinstance(row, dict):
                if resolved_by_email:
                    user_id = int(row.get("id"))
                rico_id = row.get("ricochet_user_id")
            else:
                # tuple: row[0] = id, row[1] = ricochet_user_id
                if resolved_by_email:
                    user_id = int(row[0])
                rico_id = row[1]

            has_rico = bool(rico_id)



        # main query
        if user_id is None:
            sql = """
                SELECT
                  cs.provider,
                  IFNULL(SUM(cs.inbound_calls),0)     AS inb_calls,
                  IFNULL(SUM(cs.outbound_calls),0)    AS out_calls,
                  IFNULL(SUM(cs.inbound_talk_sec),0)  AS inb_sec,
                  IFNULL(SUM(cs.outbound_talk_sec),0) AS out_sec
                FROM call_stats_union cs
                JOIN users u ON u.id = cs.reflexx_user_id
                WHERE u.manager_id = %s
                  AND cs.day BETWEEN %s AND %s
                GROUP BY cs.provider
            """
            params = (mgr_id, date_from, date_to)
        else:
            sql = """
                SELECT
                  cs.provider,
                  IFNULL(SUM(cs.inbound_calls),0)     AS inb_calls,
                  IFNULL(SUM(cs.outbound_calls),0)    AS out_calls,
                  IFNULL(SUM(cs.inbound_talk_sec),0)  AS inb_sec,
                  IFNULL(SUM(cs.outbound_talk_sec),0) AS out_sec
                FROM call_stats_union cs
                WHERE cs.reflexx_user_id = %s
                  AND cs.day BETWEEN %s AND %s
                GROUP BY cs.provider
            """
            params = (user_id, date_from, date_to)

        cur.execute(sql, params)
        rows = cur.fetchall() or []

        # fact_daily overlay (single user, single day, ONLY if user has Ricochet)
        if has_rico and user_id is not None and date_from == date_to:
            try:
                cur.execute(
                    """
                    SELECT
                        IFNULL(inbounds, 0)       AS inb,
                        IFNULL(ib_time_minutes,0) AS ib_min
                    FROM fact_daily
                    WHERE date = %s AND user_id = %s
                    """,
                    (date_from, user_id),
                )
                fd_row = cur.fetchone()
                if fd_row:
                    # fd_row might be tuple or dict
                    inb_val = fd_row[0] if not isinstance(fd_row, dict) else fd_row.get("inb", 0)
                    ib_min  = fd_row[1] if not isinstance(fd_row, dict) else fd_row.get("ib_min", 0)
                    ricochet_fd["inb"] = int(inb_val or 0)
                    ricochet_fd["sec"] = int((ib_min or 0) * 60)
            except Exception as e:
                print("[/api/calls/summary] fact_daily merge skipped:", e)

        # rico_call_metrics overlay (our clean table)
        if user_id is not None and date_from == date_to:
            try:
                cur.execute(
                    """
                    SELECT
                        IFNULL(inbound_talk_sec, 0)   AS inb_sec,
                        IFNULL(outbound_talk_sec, 0)  AS out_sec,
                        IFNULL(inbound_calls, 0)      AS inb_calls,
                        IFNULL(outbound_calls, 0)     AS out_calls
                    FROM rico_call_metrics
                    WHERE reflexx_user_id = %s
                      AND manager_id = %s
                      AND day = %s
                    """,
                    (user_id, mgr_id, date_from),
                )
                rico_exact = cur.fetchone()
            except Exception as e:
                print("[/api/calls/summary] rico_call_metrics merge skipped:", e)

    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass

    # build response
    base = {
        "inbound_calls": 0,
        "outbound_calls": 0,
        "inbound_talk_sec": 0,
        "outbound_talk_sec": 0,
    }
    out = {
        "ringcentral": dict(base),
        "ricochet": dict(base),
        "total": dict(base),
    }

    # helper to read row whether it's tuple or dict
    def getval(row, idx, key):
        if isinstance(row, dict):
            return row.get(key)
        return row[idx]

    for r in rows:
        provider = getval(r, 0, "provider")
        inb      = int(getval(r, 1, "inb_calls") or 0)
        outb     = int(getval(r, 2, "out_calls") or 0)
        inbsec   = int(getval(r, 3, "inb_sec")   or 0)
        outsec   = int(getval(r, 4, "out_sec")   or 0)

        if provider in out:
            out[provider] = {
                "inbound_calls":     inb,
                "outbound_calls":    outb,
                "inbound_talk_sec":  inbsec,
                "outbound_talk_sec": outsec,
            }

    # prefer the clean rico row
    if rico_exact is not None:
        if isinstance(rico_exact, dict):
            inb_sec   = int(rico_exact.get("inb_sec", 0) or 0)
            out_sec   = int(rico_exact.get("out_sec", 0) or 0)
            inb_calls = int(rico_exact.get("inb_calls", 0) or 0)
            out_calls = int(rico_exact.get("out_calls", 0) or 0)
        else:
            inb_sec   = int(rico_exact[0] or 0)
            out_sec   = int(rico_exact[1] or 0)
            inb_calls = int(rico_exact[2] or 0)
            out_calls = int(rico_exact[3] or 0)

        out["ricochet"]["inbound_calls"]     = inb_calls
        out["ricochet"]["outbound_calls"]    = out_calls
        out["ricochet"]["inbound_talk_sec"]  = inb_sec
        out["ricochet"]["outbound_talk_sec"] = out_sec

    # else: at least show fact_daily inbound (ONLY for users that have Ricochet)
    elif has_rico and ricochet_fd["inb"] > 0:
        out["ricochet"]["inbound_calls"]    = ricochet_fd["inb"]
        out["ricochet"]["inbound_talk_sec"] = ricochet_fd["sec"]

    # totals
    for k in base.keys():
        out["total"][k] = (out["ringcentral"][k] or 0) + (out["ricochet"][k] or 0)

    return jsonify(out)

