from flask import Blueprint, request, jsonify
import pymysql
from datetime import datetime, timedelta
from pytz import timezone
from collections import defaultdict
import json

# ✅ Define Flask Blueprint
weblogs_bp = Blueprint('weblogs', __name__)

# ✅ MySQL Configuration (Railway)
DB_HOST = "mysql.railway.internal"
DB_USER = "root"
DB_PASSWORD = "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc"
DB_NAME = "railway"

def format_time(seconds):
    """Convert seconds to hh:mm:ss format."""
    if not isinstance(seconds, (int, float)) or seconds is None:
        return "00:00:00"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02}:{minutes:02}:{secs:02}"

@weblogs_bp.route('/api/weblogs_usage', methods=['GET'])
def get_weblogs_usage():
    user_id = request.args.get('user_id')
    date_range = request.args.get('range', 'today')
    is_all = user_id == "all"

    if not user_id:
        return jsonify({"error": "Missing user_id parameter"}), 400

    if not is_all:
        try:
            user_id = int(user_id)
        except ValueError:
            return jsonify({"error": "Invalid user_id. Must be a number"}), 400

    utc_now = datetime.now(timezone("UTC"))
    la_now = utc_now.astimezone(timezone("America/Los_Angeles"))
    today = la_now.date()

    date_ranges = {
        "today": (today, today),
        "yesterday": (today - timedelta(days=1), today - timedelta(days=1)),
        "past_week": (today - timedelta(days=7), today),
        "past_month": (today - timedelta(days=30), today),
        "past_quarter": (today - timedelta(days=90), today),
        "past_year": (today - timedelta(days=365), today)
    }
    start_date, end_date = date_ranges.get(date_range, (today, today))

    try:
        conn = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()

        start_dt = datetime.combine(start_date, datetime.min.time()).replace(
            tzinfo=timezone("America/Los_Angeles")).astimezone(timezone("UTC"))
        end_dt = datetime.combine(end_date, datetime.max.time()).replace(
            tzinfo=timezone("America/Los_Angeles")).astimezone(timezone("UTC"))

        if is_all:
            cursor.execute(
                "SELECT page_time FROM activity_log WHERE timestamp BETWEEN %s AND %s",
                (start_dt, end_dt)
            )
        else:
            cursor.execute(
                "SELECT page_time FROM activity_log WHERE user_id = %s AND timestamp BETWEEN %s AND %s",
                (user_id, start_dt, end_dt)
            )

        rows = cursor.fetchall()

        # ✅ Step 1: Expanded label mapping (fuzzy match)
        label_map = {
            # Advisor Pro
            "advisorpro": "Allstate Advisor Pro",
            "advisor pro": "Allstate Advisor Pro",
            "allstate advisor pro": "Allstate Advisor Pro",
            "allstate advisor pro℠": "Allstate Advisor Pro",

            # Policy View
            "policyview": "Policy View 2.0",
            "policy view 2.0": "Policy View 2.0",
            "special auto-999700152-policy view 2.0": "Policy View 2.0",

            # eAgent
            "eagent": "eAgent",

            # Outlook
            "outlook": "Outlook",

            # Bamboo
            "policy list": "Bamboo Insurance",
            "policies >": "Bamboo Insurance",

            # Fair Plan
            "quick quote": "California Fair Plan",

            # Aegis
            "aegis": "Aegis Insurance",
            "aegis general portal": "Aegis Insurance",

            # Gateway
            "gateway": "Allstate Gateway",
            "allstate gateway": "Allstate Gateway",

            # RingCentral
            "ringcentral": "RingCentral"
        }

        total_by_label = defaultdict(float)

        # ✅ Label matcher
        def match_label(window_title):
            window_title = window_title.lower()
            for keyword in label_map:
                if keyword in window_title:
                    return label_map[keyword]
            return "Other"

        # ✅ Pre-fill all canonical labels
        for canonical_label in set(label_map.values()):
            total_by_label[canonical_label] = 0
        total_by_label["Other"] = 0

        # ✅ Parse and tally usage
        for row in rows:
            if row.get("page_time"):
                try:
                    page_data = json.loads(row["page_time"])
                    for raw_label, seconds in page_data.items():
                        seconds = float(seconds)
                        cleaned_label = raw_label.strip()
                        mapped_label = match_label(cleaned_label)
                        total_by_label[mapped_label] += seconds
                except Exception:
                    continue

        # ✅ Format JSON response
        total_time = sum(total_by_label.values()) or 1

        labels = []
        percentages = []
        times = []

        for label, seconds in sorted(total_by_label.items(), key=lambda item: item[1], reverse=True):
            labels.append(label)
            percentages.append(round((seconds / total_time) * 100, 2))
            times.append(format_time(seconds))

        cursor.close()
        conn.close()

        return jsonify({
            "data": {
                "labels": labels,
                "percentages": percentages,
                "times": times
            }
        })


    except Exception as e:
        return jsonify({"error": str(e)}), 500
