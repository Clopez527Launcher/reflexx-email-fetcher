# sales_daily_api.py
from flask import Blueprint, request, jsonify, session, current_app
from flask_login import login_required
import mysql.connector
from decimal import Decimal

sales_daily_api = Blueprint("sales_daily_api", __name__)

def get_db():
    cfg = current_app.config.get("MYSQL_CONFIG")
    return mysql.connector.connect(**cfg)

def scope():
    return session.get("manager_id"), session.get("user_id")

def num(x, cast=int):
    if x is None or x == "": return cast(0)
    return cast(x)

def dec(x):
    if x is None or x == "": return Decimal("0.00")
    return Decimal(str(x))

# POST /api/sales_daily/log
# Body: { sale_date:'YYYY-MM-DD', user_id?, mode:'add'|'set' (default 'add'),
#         vc_policies, vc_items, vc_premium, nonvc_policies, nonvc_items, nonvc_premium }
@sales_daily_api.route("/api/sales_daily/log", methods=["POST"])
@login_required
def sales_daily_log():
    mgr_id, uid = scope()
    data = request.get_json(force=True)

    sale_date = data.get("sale_date")
    if not sale_date:
        return jsonify({"error": "sale_date required"}), 400

    user_id = int(data.get("user_id") or uid or 0)
    if not user_id:
        return jsonify({"error": "user_id missing"}), 400

    mode = (data.get("mode") or "add").lower()

    vc_policies    = max(0, num(data.get("vc_policies"), int))
    vc_items       = max(0, num(data.get("vc_items"), int))
    vc_premium     = max(Decimal("0.00"), dec(data.get("vc_premium")))
    nonvc_policies = max(0, num(data.get("nonvc_policies"), int))
    nonvc_items    = max(0, num(data.get("nonvc_items"), int))
    nonvc_premium  = max(Decimal("0.00"), dec(data.get("nonvc_premium")))

    conn = get_db(); cur = conn.cursor()

    if mode == "add":
        sql = """
        INSERT INTO sales_daily (
            user_id, sale_date, vc_policies, vc_items, vc_premium,
            nonvc_policies, nonvc_items, nonvc_premium
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          vc_policies    = vc_policies    + VALUES(vc_policies),
          vc_items       = vc_items       + VALUES(vc_items),
          vc_premium     = vc_premium     + VALUES(vc_premium),
          nonvc_policies = nonvc_policies + VALUES(nonvc_policies),
          nonvc_items    = nonvc_items    + VALUES(nonvc_items),
          nonvc_premium  = nonvc_premium  + VALUES(nonvc_premium),
          updated_at     = CURRENT_TIMESTAMP
        """
    else:  # 'set'
        sql = """
        INSERT INTO sales_daily (
            user_id, sale_date, vc_policies, vc_items, vc_premium,
            nonvc_policies, nonvc_items, nonvc_premium
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          vc_policies    = VALUES(vc_policies),
          vc_items       = VALUES(vc_items),
          vc_premium     = VALUES(vc_premium),
          nonvc_policies = VALUES(nonvc_policies),
          nonvc_items    = VALUES(nonvc_items),
          nonvc_premium  = VALUES(nonvc_premium),
          updated_at     = CURRENT_TIMESTAMP
        """

    vals = (user_id, sale_date, vc_policies, vc_items, vc_premium,
            nonvc_policies, nonvc_items, nonvc_premium)
    cur.execute(sql, vals)
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True})

# GET /api/sales_daily/get?date=YYYY-MM-DD&user_id=...
@sales_daily_api.route("/api/sales_daily/get", methods=["GET"])
@login_required
def sales_daily_get():
    _, uid = scope()
    sale_date = request.args.get("date")
    user_id = int(request.args.get("user_id") or uid or 0)
    if not sale_date or not user_id:
        return jsonify({"error": "date and user_id required"}), 400

    conn = get_db(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, user_id, sale_date,
               vc_policies, vc_items, vc_premium,
               nonvc_policies, nonvc_items, nonvc_premium,
               created_at, updated_at
        FROM sales_daily
        WHERE user_id=%s AND sale_date=%s
        LIMIT 1
    """, (user_id, sale_date))
    row = cur.fetchone()
    cur.close(); conn.close()
    return jsonify({"item": row})
