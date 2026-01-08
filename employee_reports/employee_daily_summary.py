from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ✅ Import local helper (same folder)
from employee_reports.employee_emailer import employee_send_html_email

# ✅ Pull DB connection from your existing app.py
from app import get_db_connection

PACIFIC = ZoneInfo("America/Los_Angeles")


def employee_yesterday_str() -> str:
    y = (datetime.now(PACIFIC) - timedelta(days=1)).date()
    return y.strftime("%Y-%m-%d")

def hms_to_minutes(hms: str) -> float:
    """'HH:MM:SS' -> total minutes (float)"""
    try:
        parts = (hms or "00:00:00").split(":")
        if len(parts) != 3:
            return 0.0
        h, m, s = [int(x) for x in parts]
        return (h * 60) + m + (s / 60.0)
    except:
        return 0.0


def build_employee_coaching(nickname: str, call_stats: dict, eprops: int, buckets: dict) -> dict:
    """
    Returns:
      { well: str, improve: str, action: str }
    """
    outbounds = int(call_stats.get("outbounds") or 0)
    ob_minutes = hms_to_minutes(call_stats.get("ob_time") or "00:00:00")

    phone_grade = (buckets.get("phone_grade") or "Average").strip()
    quote_grade = (buckets.get("quote_grade") or "Average").strip()
    movement_grade = (buckets.get("movement_grade") or "Average").strip()

    # -------------------------
    # 1) CALLS: outbound volume
    # -------------------------
    if outbounds >= 100:
        outbounds_msg = f"You made **{outbounds} outbounds** — that’s solid volume."
        outbounds_level = "great"
    elif outbounds >= 50:
        outbounds_msg = f"You made **{outbounds} outbounds** — decent, keep pushing."
        outbounds_level = "ok"
    elif outbounds >= 40:
        outbounds_msg = f"You made **{outbounds} outbounds** — borderline. Let’s aim higher."
        outbounds_level = "low"
    else:
        outbounds_msg = f"You made **{outbounds} outbounds** — too low. We need more attempts."
        outbounds_level = "bad"

    # -------------------------
    # 1) CALLS: outbound talk time
    # -------------------------
    if ob_minutes >= 90:
        obtalk_msg = f"Your outbound talk time was **{call_stats.get('ob_time','00:00:00')}** — great (right in the 1:30+ range)."
        obtalk_level = "great"
    elif ob_minutes >= 45:
        obtalk_msg = f"Your outbound talk time was **{call_stats.get('ob_time','00:00:00')}** — okay, but we want to build this toward 1:30–2:00."
        obtalk_level = "ok"
    elif ob_minutes >= 30:
        obtalk_msg = f"Your outbound talk time was **{call_stats.get('ob_time','00:00:00')}** — a bit light."
        obtalk_level = "low"
    else:
        obtalk_msg = f"Your outbound talk time was **{call_stats.get('ob_time','00:00:00')}** — too low. We need more real conversations."
        obtalk_level = "bad"

    # -------------------------
    # 2) E-PROPOSALS
    # -------------------------
    if eprops >= 8:
        eprop_msg = f"You sent **{eprops} E-Proposal(s)** — great follow-through."
        eprop_level = "great"
    elif eprops >= 5:
        eprop_msg = f"You sent **{eprops} E-Proposals** — good. Let’s push for 8+."
        eprop_level = "ok"
    else:
        eprop_msg = f"You sent **{eprops} E-Proposals** — not enough. Let’s target 5–7 minimum."
        eprop_level = "bad"

    # -------------------------
    # 3) GRADES (focus Phone + Quote, avoid Movement unless Poor)
    # -------------------------
    def grade_msg(bucket_name: str, grade: str) -> str:
        if grade == "Excellent":
            return f"{bucket_name} was **Excellent** — keep doing what you’re doing."
        if grade == "Above Average":
            return f"{bucket_name} was **Above Average** — strong pace."
        if grade == "Average":
            return f"{bucket_name} was **Average** — okay baseline, room to level up."
        if grade == "Below Average":
            return f"{bucket_name} was **Below Average** — we should tighten this up."
        return f"{bucket_name} was **Poor** — let’s fix this quickly."

    phone_msg = grade_msg("Phone", phone_grade)
    quote_msg = grade_msg("Quote", quote_grade)

    movement_msg = ""
    if movement_grade == "Poor":
        movement_msg = "Movement was **Poor** — you may need some training navigating a few applications to reduce friction and speed up workflows."

    # -------------------------
    # Build "What you did well"
    # -------------------------
    well_parts = []
    # praise the best levers that were actually good
    if outbounds_level in ("ok", "great"):
        well_parts.append(outbounds_msg)
    if obtalk_level in ("ok", "great"):
        well_parts.append(obtalk_msg)
    if eprop_level in ("ok", "great"):
        well_parts.append(eprop_msg)
    if phone_grade in ("Above Average", "Excellent"):
        well_parts.append(phone_msg)
    if quote_grade in ("Above Average", "Excellent"):
        well_parts.append(quote_msg)

    if not well_parts:
        well_parts.append("You showed up — now let’s tighten the plan and get momentum back.")

    # -------------------------
    # Build "What to improve"
    # -------------------------
    improve_parts = []
    if outbounds_level in ("bad", "low"):
        improve_parts.append(outbounds_msg)
    if obtalk_level in ("bad", "low"):
        improve_parts.append(obtalk_msg)
    if eprop_level == "bad":
        improve_parts.append(eprop_msg)

    # Focus recommendations around Phone/Quote grades
    if phone_grade in ("Below Average", "Poor"):
        improve_parts.append("Phone activity needs attention — set a tight call block and protect it.")
    if quote_grade in ("Below Average", "Poor"):
        improve_parts.append("Quote activity needs attention — break quoting into smaller sessions so it doesn’t stack up.")

    if movement_msg:
        improve_parts.append(movement_msg)

    if not improve_parts:
        improve_parts.append("Main focus: keep consistency — replicate what worked and slightly raise targets.")

    # -------------------------
    # Build "One action for today"
    # (your A/B strategy idea)
    # -------------------------
    action = (
        "Try an experiment: for the next **2–3 days**, make most of your calls **before noon**. "
        "Then for the next **2–3 days**, shift calls to **after noon**. Compare talk time + e-proposals and keep the better schedule."
    )

    # If they’re really low, make it more urgent + specific
    if outbounds_level == "bad" or obtalk_level == "bad":
        action = (
            "Do one protected call block today: **60 minutes, no distractions**. "
            "Goal: raise outbound talk time. After that block, immediately send e-proposals while the conversations are fresh."
        )

    return {
        "well": " ".join(well_parts),
        "improve": " ".join(improve_parts),
        "action": action
    }


def employee_get_enabled_users():
    """
    Returns list of dicts:
      {id, email, nickname, manager_id}
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, email, COALESCE(nickname, email) AS nickname, manager_id
        FROM users
        WHERE role = 'user'
          AND is_active = 1
          AND staff_daily_summary_enabled = 1
          AND email IS NOT NULL
          AND email <> ''
        ORDER BY manager_id, id
    """)

    rows_raw = cur.fetchall()

    # ✅ connector-safe mapping
    if rows_raw and isinstance(rows_raw[0], dict):
        users = rows_raw
    else:
        cols = [d[0] for d in cur.description]
        users = [dict(zip(cols, r)) for r in rows_raw]

    cur.close()
    conn.close()
    return users


def employee_fetch_call_stats_yesterday(user_id: int):
    """
    Pull yesterday's call stats from call_stats_union.
    Returns dict:
      { inbounds, outbounds, ib_time, ob_time }
    """
    def sec_to_hms(sec):
        sec = int(sec or 0)
        h = sec // 3600
        m = (sec % 3600) // 60
        s = sec % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    yday = (datetime.now(PACIFIC) - timedelta(days=1)).date()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
          COALESCE(SUM(inbound_calls), 0)        AS inbounds,
          COALESCE(SUM(outbound_calls), 0)       AS outbounds,
          COALESCE(SUM(inbound_talk_sec), 0)     AS inbound_talk_sec,
          COALESCE(SUM(outbound_talk_sec), 0)    AS outbound_talk_sec
        FROM call_stats_union
        WHERE reflexx_user_id = %s
          AND day = %s
    """, (user_id, yday))

    row = cur.fetchone()
    cur.close()
    conn.close()

    # dict mode
    if isinstance(row, dict):
        return {
            "inbounds": row.get("inbounds", 0),
            "outbounds": row.get("outbounds", 0),
            "ib_time": sec_to_hms(row.get("inbound_talk_sec", 0)),
            "ob_time": sec_to_hms(row.get("outbound_talk_sec", 0)),
        }

    # tuple mode
    return {
        "inbounds": row[0] if row else 0,
        "outbounds": row[1] if row else 0,
        "ib_time": sec_to_hms(row[2] if row else 0),
        "ob_time": sec_to_hms(row[3] if row else 0),
    }


def employee_fetch_eproposals_yesterday(user_id: int) -> int:
    """
    Pull yesterday's e-proposal count from v_eproposal_daily_pt.
    Returns int: eproposal_count (defaults to 0).
    """
    yday = (datetime.now(PACIFIC) - timedelta(days=1)).date()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT COALESCE(eproposal_count, 0) AS cnt
        FROM v_eproposal_daily_pt
        WHERE user_id = %s
          AND pt_date = %s
        LIMIT 1
    """, (user_id, yday))

    row = cur.fetchone()
    cur.close()
    conn.close()

    # dict mode
    if isinstance(row, dict):
        return int(row.get("cnt") or 0)

    # tuple mode
    if not row:
        return 0
    return int(row[0] or 0)

def employee_fetch_bucket_zscores_yesterday(user_id: int):
    """
    Pull Z-scores (7-day) from fact_daily for yesterday (PT date)
    and compute grades using the correct scale:

      if zNum > 1.5  -> Excellent
      if zNum > 0.5  -> Above Average
      if zNum >= -0.5 -> Average
      if zNum >= -1.5 -> Below Average
      else -> Poor
    """
    yday = (datetime.now(PACIFIC) - timedelta(days=1)).date()

    def grade_from_z(z):
        try:
            zNum = float(z)
        except:
            zNum = 0.0

        if zNum > 1.5:
            return "Excellent"
        if zNum > 0.5:
            return "Above Average"
        if zNum >= -0.5:
            return "Average"
        if zNum >= -1.5:
            return "Below Average"
        return "Poor"

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
          phone_ce_l7_z,
          quote_ce_l7_z,
          movement_ce_l7_z
        FROM fact_daily
        WHERE user_id = %s
          AND date = %s
        LIMIT 1
    """, (user_id, yday))

    row = cur.fetchone()
    cur.close()
    conn.close()

    # default if missing
    phone_z = 0.0
    quote_z = 0.0
    movement_z = 0.0

    if row:
        if isinstance(row, dict):
            phone_z = row.get("phone_ce_l7_z") or 0.0
            quote_z = row.get("quote_ce_l7_z") or 0.0
            movement_z = row.get("movement_ce_l7_z") or 0.0
        else:
            phone_z = row[0] or 0.0
            quote_z = row[1] or 0.0
            movement_z = row[2] or 0.0

    return {
        "phone_grade": grade_from_z(phone_z),
        "quote_grade": grade_from_z(quote_z),
        "movement_grade": grade_from_z(movement_z),
        "phone_z": float(phone_z or 0),
        "quote_z": float(quote_z or 0),
        "movement_z": float(movement_z or 0),
    }


def employee_build_email_html(nickname: str, call_stats: dict, eprops: int, buckets: dict) -> str:
    yday = employee_yesterday_str()
    coaching = build_employee_coaching(nickname, call_stats, eprops, buckets)
    
    def zfmt(x):
        try:
            x = float(x)
        except:
            x = 0.0
        sign = "+" if x > 0 else ""
        return f"{sign}{x:.2f}"

    return f"""
    <html>
    <body style="font-family: Arial, sans-serif; background:#0b1220; color:#e9f6ff; padding:20px;">
      <div style="max-width:700px; margin:0 auto; background:#0e151d; border:1px solid rgba(255,255,255,0.08); border-radius:14px; padding:18px;">
        <h2 style="margin:0 0 6px 0;">Your Reflexx Summary — {yday}</h2>
        <div style="color:rgba(233,246,255,0.75); margin-bottom:16px;">
          Hi {nickname}, here’s a quick summary of yesterday.
        </div>

        <h3 style="margin:16px 0 8px 0;">1) Call Stats</h3>
        <table style="width:100%; border-collapse:collapse;">
          <tr><td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Inbounds</td>
              <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:right;">{call_stats.get("inbounds",0)}</td></tr>
          <tr><td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Outbounds</td>
              <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:right;">{call_stats.get("outbounds",0)}</td></tr>
          <tr><td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Inbound Talk Time</td>
              <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:right;">{call_stats.get("ib_time","00:00:00")}</td></tr>
          <tr><td style="padding:8px;">Outbound Talk Time</td>
              <td style="padding:8px; text-align:right;">{call_stats.get("ob_time","00:00:00")}</td></tr>
        </table>

        <h3 style="margin:16px 0 8px 0;">2) E-Proposals</h3>
        <div style="font-size:16px; padding:10px; background:rgba(0,230,230,0.10); border:1px solid rgba(0,230,230,0.25); border-radius:10px;">
          Total sent: <b>{eprops}</b>
        </div>

        <h3 style="margin:16px 0 8px 0;">3) Performance Grades & Z-Scores (Last 7 days)</h3>
        <table style="width:100%; border-collapse:collapse;">
          <tr style="color:#bffcff;">
            <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Bucket</th>
            <th style="text-align:right; padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Grade</th>
            <th style="text-align:right; padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Z-Score</th>
          </tr>
          <tr>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Phone</td>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:right;">{buckets.get("phone_grade","-")}</td>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:right;">{zfmt(buckets.get("phone_z",0))}</td>
          </tr>
          <tr>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Quote</td>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:right;">{buckets.get("quote_grade","-")}</td>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:right;">{zfmt(buckets.get("quote_z",0))}</td>
          </tr>
          <tr>
            <td style="padding:8px;">Movement</td>
            <td style="padding:8px; text-align:right;">{buckets.get("movement_grade","-")}</td>
            <td style="padding:8px; text-align:right;">{zfmt(buckets.get("movement_z",0))}</td>
          </tr>
        </table>

        coaching = build_employee_coaching(nickname, call_stats, eprops, buckets)
        
        <h3 style="margin:16px 0 8px 0;">4) Coaching</h3>
        <div style="color:rgba(233,246,255,0.85); line-height:1.4;">
          <b>What you did well:</b> {coaching["well"]}<br/><br/>
          <b>What to improve:</b> {coaching["improve"]}<br/><br/>
          <b>One action for today:</b> {coaching["action"]}
        </div>

        <div style="margin-top:18px; color:rgba(233,246,255,0.55); font-size:12px;">
          Sent by Reflexx • If you think any numbers are wrong, tell your manager.
        </div>
      </div>
    </body>
    </html>
    """


def employee_send_daily_summaries(dry_run: bool = False):
    users = employee_get_enabled_users()
    print(f"[EmployeeDaily] enabled users: {len(users)}")

    for u in users:
        user_id = int(u["id"])
        email = (u.get("email") or "").strip()
        nickname = (u.get("nickname") or email or f"User {user_id}").strip()

        if not email:
            print(f"[EmployeeDaily] skipping user_id={user_id} (missing email)")
            continue

        call_stats = employee_fetch_call_stats_yesterday(user_id)
        eprops = employee_fetch_eproposals_yesterday(user_id)
        buckets = employee_fetch_bucket_zscores_yesterday(user_id)

        subject = f"Reflexx Summary — {employee_yesterday_str()}"
        html = employee_build_email_html(nickname, call_stats, eprops, buckets)

        if dry_run:
            print(f"[EmployeeDaily] DRY RUN: would send to {email} (user_id={user_id})")
            continue

        try:
            employee_send_html_email(email, subject, html)
            print(f"[EmployeeDaily] sent to {email} (user_id={user_id})")
        except Exception as e:
            print(f"[EmployeeDaily] ERROR sending to {email} (user_id={user_id}): {e}")
