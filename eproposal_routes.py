# eproposal_routes.py  ✅ DROP-IN REPLACEMENT
# ------------------------------------------------------------
# Fixes circular import by NOT importing app.py at import-time.
# We import get_db_connection lazily inside a helper function.
# ------------------------------------------------------------

from flask import Blueprint, request, jsonify, send_file, session
import csv
import io
from datetime import datetime

eproposal_bp = Blueprint("eproposal", __name__)

# ✅ Lazy DB getter (prevents circular import on boot)
def db():
    from app import get_db_connection
    return get_db_connection()

def parse_dt(v):
    """Parse '12/1/2025 3:22:10 PM' -> datetime or None"""
    try:
        if not v:
            return None
        v = str(v).strip()
        if not v:
            return None
        return datetime.strptime(v, "%m/%d/%Y %I:%M:%S %p")
    except Exception:
        return None

@eproposal_bp.route("/api/eproposal/upload", methods=["POST"])
def upload_eproposal_history():
    manager_id = session.get("manager_id")
    user_id = session.get("user_id") or session.get("id")

    if not manager_id:
        return jsonify({"success": False, "error": "Not logged in"}), 401

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"success": False, "error": "No file provided"}), 400

    raw = f.read()
    if not raw:
        return jsonify({"success": False, "error": "Empty file"}), 400

    # ✅ decode CSV (handles BOM)
    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    rows = []
    for r in reader:
        rows.append((
            (r.get("Agency Number") or "").strip() or None,
            (r.get("Last Name") or "").strip() or None,
            (r.get("First Name") or "").strip() or None,
            (r.get("Type") or "").strip() or None,
            (r.get("Sender") or "").strip() or None,
            parse_dt(r.get("Customer Viewed", "")),
            parse_dt(r.get("Created Date", "")),
            (r.get("Quotes") or "").strip() or None,
            (r.get("E-Mail Address") or "").strip() or None
        ))

    if not rows:
        return jsonify({"success": False, "error": "No data rows"}), 400

    conn = db()
    cur = conn.cursor()

    try:
        # 1) Save upload metadata + file blob
        cur.execute("""
            INSERT INTO eproposal_uploads
              (manager_id, uploaded_by_user_id, original_name, file_size, content_type, file_blob)
            VALUES
              (%s, %s, %s, %s, %s, %s)
        """, (
            manager_id,
            user_id,
            f.filename,
            len(raw),
            (f.mimetype or "text/csv"),
            raw
        ))
        upload_id = cur.lastrowid

        # 2) Bulk insert the history rows
        cur.executemany("""
            INSERT INTO eproposal_history
              (upload_id, agency_number, last_name, first_name, record_type,
               sender_allstate_user_id, customer_viewed_at, created_at, quotes, email_address,
               uploaded_by_user_id, manager_id)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [
            (upload_id, *row, user_id, manager_id)
            for row in rows
        ])

        conn.commit()
        return jsonify({"success": True, "upload_id": upload_id, "rows": len(rows)})

    except Exception as e:
        conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@eproposal_bp.route("/api/eproposal/list")
def list_eproposal_uploads():
    manager_id = session.get("manager_id")
    if not manager_id:
        return jsonify({"page": 1, "total_pages": 1, "items": []}), 200

    page = int(request.args.get("page", 1))
    per_page = 5
    offset = (page - 1) * per_page

    conn = db()
    cur = conn.cursor()

    try:
        cur.execute(
            "SELECT COUNT(*) FROM eproposal_uploads WHERE manager_id=%s",
            (manager_id,)
        )
        row = cur.fetchone()
        total = int(row[0]) if row and row[0] is not None else 0
        total_pages = max(1, (total + per_page - 1) // per_page)

        cur.execute("""
            SELECT id, original_name, file_size, uploaded_at
            FROM eproposal_uploads
            WHERE manager_id=%s
            ORDER BY uploaded_at DESC
            LIMIT %s OFFSET %s
        """, (manager_id, per_page, offset))

        rows = cur.fetchall() or []

        items = []
        for r in rows:
            # r = (id, original_name, file_size, uploaded_at)
            items.append({
                "id": r[0],
                "original_name": r[1],
                "file_size": r[2],
                "uploaded_at": r[3].isoformat() if r[3] else None
            })

        return jsonify({"page": page, "total_pages": total_pages, "items": items})

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


@eproposal_bp.route("/eproposal/download/<int:upload_id>")
def download_eproposal(upload_id):
    manager_id = session.get("manager_id")
    if not manager_id:
        return "Not logged in", 401

    conn = db()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT original_name, content_type, file_blob
            FROM eproposal_uploads
            WHERE id=%s AND manager_id=%s
        """, (upload_id, manager_id))

        row = cur.fetchone()
        # row = (original_name, content_type, file_blob)
        if not row or not row[2]:
            return "Not found", 404

        original_name = row[0] or "eproposal.csv"
        content_type  = row[1] or "text/csv"
        file_blob     = row[2]

        return send_file(
            io.BytesIO(file_blob),
            mimetype=content_type,
            as_attachment=True,
            download_name=original_name
        )

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
