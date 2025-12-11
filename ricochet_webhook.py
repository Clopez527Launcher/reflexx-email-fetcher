# ricochet_webhook.py
from flask import Blueprint, request, jsonify
import os, hashlib, json
from datetime import datetime

# ✅ MySQL Configuration (Railway)
DB_HOST = "mysql.railway.internal"
DB_USER = "root"
DB_PASSWORD = "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc"  # rotate later
DB_NAME = "railway"
DB_PORT = 3306

MYSQL_CONFIG = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": DB_PORT,
}

# Flask blueprint
ricochet_bp = Blueprint("ricochet_webhook", __name__)
EXPECTED = os.getenv("RICOCHET_WEBHOOK_TOKEN")

# --- constants for call_type coming from Ricochet ---
CALL_TYPE_OUTBOUND = 5
CALL_TYPE_INBOUND  = 7

# --- DB helpers using PyMySQL ---
import pymysql

def _conn():
    return pymysql.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
        port=MYSQL_CONFIG["port"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
        autocommit=True,
    )

def db_exec(sql, params=None):
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
    except Exception as e:
        print("[ricochet][db_exec] error:", e, "| sql:", sql, "| params:", params)
        raise

def db_scalar(sql, params=None):
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                row = cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        print("[ricochet][db_scalar] error:", e, "| sql:", sql, "| params:", params)
        raise

def already_counted_call(rc_user_id: int, call_id: str, rc_date_yyyymmdd: str) -> bool:
    """
    We log EVERY Ricochet event into ricochet_events first.
    So: if we see the same (ricochet user, call_id, ricochet day) more than once,
    it means this is a later update for the same call -> don't add to metrics again.
    """
    try:
        cnt = db_scalar(
            """
            SELECT COUNT(*) 
            FROM ricochet_events
            WHERE user_id = %s
              AND call_id = %s
              AND rc_date_yyyymmdd = %s
            """,
            (rc_user_id, call_id, rc_date_yyyymmdd),
        )
        return (cnt or 0) > 1  # >1 because we just inserted this very event
    except Exception as e:
        print("[ricochet] already_counted_call failed:", e)
        # if check fails, be safe and say "not counted" so we don't lose data
        return False
        

# --- input coercion (prevents '' -> INT errors) ---
def to_int_or_none(v):
    try:
        if v is None: return None
        if isinstance(v, bool): return int(v)
        if isinstance(v, (int, float)): return int(v)
        s = str(v).strip()
        if s == "": return None
        return int(float(s))
    except Exception:
        return None

def to_int_or_zero(v):
    x = to_int_or_none(v)
    return 0 if x is None else x

# --- utility funcs ---
def seen_event(eid: str) -> bool:
    return bool(db_scalar("SELECT 1 FROM ricochet_events WHERE event_id=%s", (eid,)))

def record_event(eid: str, payload: dict):
    user_id       = to_int_or_none(payload.get("user_id"))
    lead_id       = to_int_or_none(payload.get("lead_id"))
    call_duration = to_int_or_none(payload.get("call_duration"))
    call_type     = to_int_or_none(payload.get("call_type"))
    rc_date       = (payload.get("current_date") or None)
    rc_time       = (payload.get("current_time") or None)
    call_id       = (payload.get("call_id") or None)
    mgr_id        = to_int_or_none(payload.get("reflexx_manager_id"))

    db_exec("""
      INSERT INTO ricochet_events
        (event_id, call_id, reflexx_manager_id, user_id, lead_id,
         call_duration, call_type, rc_date_yyyymmdd, rc_time_local, raw_json)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      ON DUPLICATE KEY UPDATE event_id = event_id
    """, (
      eid, call_id, mgr_id, user_id, lead_id,
      call_duration, call_type, rc_date, rc_time,
      json.dumps(payload, separators=(',',':'))
    ))

def map_reflexx_user(ricochet_user_id, manager_reflexx_id=None, extension=None):
    """
    Resolve to our users.id using (manager_id, ricochet_user_id) first.
    Fallbacks: by ricochet_user_id alone, then by extension.
    """
    if manager_reflexx_id is not None:
        uid = db_scalar(
            "SELECT id FROM users WHERE manager_id=%s AND ricochet_user_id=%s",
            (manager_reflexx_id, ricochet_user_id),
        )
        if uid:
            return uid

    uid = db_scalar("SELECT id FROM users WHERE ricochet_user_id=%s", (ricochet_user_id,))
    if uid:
        return uid

    if extension:
        uid = db_scalar("SELECT id FROM users WHERE extension=%s", (str(extension),))
        if uid:
            return uid

    return None

def yyyymmdd_to_date(s: str) -> str:
    # "20250913" -> "2025-09-13"
    return datetime.strptime(s, "%Y%m%d").date().isoformat()

def add_to_daily(reflexx_user_id: int, manager_id: int, day_iso: str, inbound_add: int, outbound_add: int):
    # rico_call_metrics PK is (reflexx_user_id, day) — ON DUPLICATE KEY will hit that
    db_exec("""
      INSERT INTO rico_call_metrics
        (reflexx_user_id, manager_id, day, inbound_talk_sec, outbound_talk_sec, inbound_calls, outbound_calls)
      VALUES (%s,%s,%s,%s,%s,%s,%s)
      ON DUPLICATE KEY UPDATE
        inbound_talk_sec  = inbound_talk_sec  + VALUES(inbound_talk_sec),
        outbound_talk_sec = outbound_talk_sec + VALUES(outbound_talk_sec),
        inbound_calls     = inbound_calls     + VALUES(inbound_calls),
        outbound_calls    = outbound_calls    + VALUES(outbound_calls)
    """, (
      reflexx_user_id, manager_id, day_iso,
      inbound_add, outbound_add,
      1 if inbound_add  else 0,
      1 if outbound_add else 0
    ))

# --- webhook route ---
@ricochet_bp.route("/api/ricochet/webhook", methods=["POST"])
def ricochet_webhook():
    # 0) shared secret
    if request.headers.get("X-Reflexx-Token") != EXPECTED:
        return jsonify({"error": "unauthorized"}), 401

    # 1) parse body (Ricochet is sending application/json now)
    payload = request.get_json(silent=True) or {}
    raw     = request.get_data(cache=False, as_text=True)
    ctype   = (request.headers.get("Content-Type") or "").lower()

    # 2) stable event id (hash of raw body)
    event_id = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # 3) quick preview to logs
    print(f"[ricochet] ctype: {ctype} | event_id: {event_id} | json: {payload}")

    # 4) idempotency: skip if seen
    if seen_event(event_id):
        return jsonify({"ok": True, "dedup": True}), 200

    # 5) record raw event once for audit/replay (with sanitized ints)
    record_event(event_id, payload)

    # 6) map user and update daily counters
    try:
        mgr_reflexx_id = to_int_or_none(payload.get("reflexx_manager_id"))
        rc_user_id     = to_int_or_zero(payload.get("user_id"))
        ext            = payload.get("extension")

        reflexx_id = map_reflexx_user(
            rc_user_id,
            manager_reflexx_id=mgr_reflexx_id,
            extension=ext
        )
        if not reflexx_id:
            print("[ricochet] unmapped user_id:", rc_user_id, "mgr:", mgr_reflexx_id, "ext:", ext)
            return jsonify({"ok": True, "unmapped_user": rc_user_id, "mgr": mgr_reflexx_id}), 200

        call_type = to_int_or_zero(payload.get("call_type"))
        dur       = to_int_or_zero(payload.get("call_duration"))

        # choose the day key (same as before)
        if payload.get("current_date"):
            day_iso = yyyymmdd_to_date(str(payload["current_date"]))
        else:
            day_iso = (payload.get("current_time") or "")[:10]

        # ---- SAFE inbound detection ----
        inbound_types = {10}  # always treat 10 as inbound
        try:
            # add the normal inbound constant if it exists
            inbound_types.add(CALL_TYPE_INBOUND)
        except NameError:
            print("[ricochet] CALL_TYPE_INBOUND not defined, using {10} only")

        inbound_add  = dur if call_type in inbound_types else 0
        outbound_add = dur if call_type in (1, 2, 3, 4, 5, 6) else 0

        # prefer truth from DB
        manager_id = db_scalar("SELECT manager_id FROM users WHERE id=%s", (reflexx_id,))
        manager_id = int(manager_id or 0)

        print("[ricochet] parsed:",
              "reflexx_id=", reflexx_id,
              "mgr=", manager_id,
              "date=", day_iso,
              "call_type=", call_type,
              "dur=", dur,
              "inbound_add=", inbound_add,
              "outbound_add=", outbound_add)

        # we just inserted the raw event, so now we can check if this call was already seen today
        rc_user_raw = rc_user_id  # ricochet user id from payload (NOT reflexx id)
        rc_call_id  = payload.get("call_id") or ""
        rc_date_raw = payload.get("current_date") or day_iso.replace("-", "")  # e.g. "20251104"

        is_dup = False
        if rc_call_id:
            is_dup = already_counted_call(rc_user_raw, rc_call_id, rc_date_raw)

        if is_dup:
            print("[ricochet] duplicate for same call/day, skipping metrics:",
                  rc_user_raw, rc_call_id, rc_date_raw)
        else:
            if inbound_add or outbound_add:
                add_to_daily(reflexx_id, manager_id, day_iso, inbound_add, outbound_add)
                print("[ricochet] add_to_daily OK")
            else:
                print("[ricochet] ignored call_type:", call_type, "dur:", dur)


    except Exception as e:
        # DO NOT hide this anymore — print it loud
        import traceback
        print("[ricochet] handler error:", e)
        traceback.print_exc()
        # still return 200 so Ricochet doesn't spam retries
        return jsonify({"ok": False, "error": str(e)}), 200

    return jsonify({"ok": True}), 200

