from flask import Blueprint, request, jsonify, send_file, session
import csv
import io
from datetime import datetime

from app import get_db_connection   # ✅ same helper you already use

eproposal_bp = Blueprint("eproposal", __name__)

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

    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    def parse_dt(v):
        try:
            return datetime.strptime(v.strip(), "%m/%d/%Y %I:%M:%S %p") if v else None
        except:
            return None

    rows = []
    for r in reader:
        rows.append((
            r.get("Agency Number"),
            r.get("Last Name"),
            r.get("First Name"),
            r.get("Type"),
            r.get("Sender"),
            parse_dt(r.get("Customer Viewed", "")),
            parse_dt(r.get("Created Date", "")),
            r.get("Quotes"),
            r.get("E-Mail Address")
        ))

    if not rows:
        return jsonify({"success": False, "error": "No data rows"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO eproposal_uploads
            (manager_id, uploaded_by_user_id, original_name, file_size, content_type, file_blob)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (manager_id, user_id, f.filename, len(raw), f.mimetype, raw))

        upload_id = cur.lastrowid

        cur.executemany("""
            INSERT INTO eproposal_history
            (upload_id, agency_number, last_name, first_name, record_type,
             sender_allstate_user_id, customer_viewed_at, created_at, quotes, email_address,
             uploaded_by_user_id, manager_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [
            (upload_id, *row, user_id, manager_id) for row in rows
        ])

        conn.commit()
        return jsonify({"success": True, "rows": len(rows)})

    except Exception as e:
        conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        cur.close()
        conn.close()

@eproposal_bp.route("/api/eproposal/list")
def list_eproposal_uploads():
    manager_id = session.get("manager_id")
    page = int(request.args.get("page", 1))
    per_page = 5
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT COUNT(*) n FROM eproposal_uploads WHERE manager_id=%s", (manager_id,))
    total = cur.fetchone()["n"]
    total_pages = max(1, (total + per_page - 1) // per_page)

    cur.execute("""
        SELECT id, original_name, file_size, uploaded_at
        FROM eproposal_uploads
        WHERE manager_id=%s
        ORDER BY uploaded_at DESC
        LIMIT %s OFFSET %s
    """, (manager_id, per_page, offset))

    items = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify({
        "page": page,
        "total_pages": total_pages,
        "items": items
    })


@eproposal_bp.route("/eproposal/download/<int:upload_id>")
def download_eproposal(upload_id):
    manager_id = session.get("manager_id")

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT original_name, content_type, file_blob
        FROM eproposal_uploads
        WHERE id=%s AND manager_id=%s
    """, (upload_id, manager_id))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return "Not found", 404

    return send_file(
        io.BytesIO(row["file_blob"]),
        mimetype=row["content_type"],
        as_attachment=True,
        download_name=row["original_name"]
    )
