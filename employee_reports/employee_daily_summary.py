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
      {
        well: str,
        improve: str,
        actions: list[str],   # 10+ direct actions for TODAY
        focus: str            # 1-line focus for today
      }
    """
    outbounds = int(call_stats.get("outbounds") or 0)
    inbounds  = int(call_stats.get("inbounds") or 0)

    ob_minutes = hms_to_minutes(call_stats.get("ob_time") or "00:00:00")
    ib_minutes = hms_to_minutes(call_stats.get("ib_time") or "00:00:00")

    phone_grade = (buckets.get("phone_grade") or "Average").strip()
    quote_grade = (buckets.get("quote_grade") or "Average").strip()
    movement_grade = (buckets.get("movement_grade") or "Average").strip()

    # -------------------------
    # Levels (simple)
    # -------------------------
    def level_outbounds(x: int) -> str:
        if x >= 100: return "great"
        if x >= 50:  return "ok"
        if x >= 40:  return "low"
        return "bad"

    def level_obtalk(m: float) -> str:
        if m >= 90: return "great"
        if m >= 45: return "ok"
        if m >= 30: return "low"
        return "bad"

    def level_eprops(x: int) -> str:
        if x >= 8: return "great"
        if x >= 5: return "ok"
        return "bad"

    ob_level = level_outbounds(outbounds)
    obt_level = level_obtalk(ob_minutes)
    ep_level = level_eprops(eprops)

    # -------------------------
    # What you did well (short)
    # -------------------------
    well_parts = []

    if ob_level in ("ok", "great"):
        well_parts.append(f"Outbounds: **{outbounds}**.")
    if obt_level in ("ok", "great"):
        well_parts.append(f"Outbound talk: **{call_stats.get('ob_time','00:00:00')}**.")
    if ep_level in ("ok", "great"):
        well_parts.append(f"E-Proposals: **{eprops}**.")
    if phone_grade in ("Above Average", "Excellent"):
        well_parts.append(f"Phone grade: **{phone_grade}**.")
    if quote_grade in ("Above Average", "Excellent"):
        well_parts.append(f"Quote grade: **{quote_grade}**.")

    if not well_parts:
        well_parts.append("No strong lever yesterday — today we reset with a tighter plan.")

    # -------------------------
    # What to improve (short + direct)
    # -------------------------
    improve_parts = []

    if ob_level in ("bad", "low"):
        improve_parts.append(f"Outbounds were low (**{outbounds}**).")
    if obt_level in ("bad", "low"):
        improve_parts.append(f"Outbound talk was low (**{call_stats.get('ob_time','00:00:00')}**).")
    if ep_level == "bad":
        improve_parts.append(f"E-Proposals were low (**{eprops}**).")
    if phone_grade in ("Below Average", "Poor"):
        improve_parts.append(f"Phone grade is **{phone_grade}** (needs a protected call block).")
    if quote_grade in ("Below Average", "Poor"):
        improve_parts.append(f"Quote grade is **{quote_grade}** (needs tighter quote workflow).")
    if movement_grade == "Poor":
        improve_parts.append("Movement grade is **Poor** (workflow friction is slowing you down).")

    if not improve_parts:
        improve_parts.append("Main goal: keep consistency and push targets slightly higher.")

    # -------------------------
    # TODAY: 10+ tailored actions (direct, not motivational)
    # -------------------------
    actions = []

    # Always include a structure plan (people need a schedule)
    actions.append("Block **2 call sessions** today: **9:00–10:00** and **1:30–2:15** (calendar it now).")
    actions.append("During call blocks: **no quoting**. Calls only. Quotes happen after the block.")

    # Outbounds-specific
    if ob_level == "bad":
        actions.append("Set a floor: **40 outbounds by lunch**. If you miss it, extend the call block 15 minutes.")
        actions.append("Use a simple pace rule: **1 outbound every 2 minutes** (timer on).")
    elif ob_level == "low":
        actions.append("Set a floor: **25 outbounds by lunch**. Hit it before any admin work.")
    else:
        actions.append("Keep pace: set a floor of **50 outbounds** again today to stay consistent.")

    # Talk-time specific (conversation quality)
    if obt_level in ("bad", "low"):
        actions.append("On every connect, ask these 2 questions before quoting: **“What’s driving the change?” + “What coverage matters most?”**")
        actions.append("Goal for the day: **+15 minutes outbound talk time** vs yesterday (track it after each call block).")
        actions.append("If you get short calls: pivot to **2 follow-up questions** instead of ending the call.")
    else:
        actions.append("Keep talk time strong: aim for **3 meaningful connects** per call block.")

    # E-proposals (conversion)
    if ep_level == "bad":
        actions.append("After each call block, do a **20-minute follow-up sprint**: send **at least 3 E-Proposals** immediately.")
        actions.append("Write 1 follow-up template and reuse it all day: **Quote sent → next step + deadline**.")
    elif ep_level == "ok":
        actions.append("Push E-Proposals to **8 today** by doing a follow-up sprint after each call block.")
    else:
        actions.append("Repeat what worked: send E-Proposals **same day** while the conversation is fresh.")

    # Quote grade actions
    if quote_grade in ("Below Average", "Poor"):
        actions.append("Run quoting in **2 small batches** (ex: **10:15–10:45** and **2:20–2:45**) so it doesn’t stack up.")
        actions.append("Start every quote with the same checklist: **drivers → vehicles → prior → discounts → coverage** (no skipping).")
    else:
        actions.append("Keep quote output steady: **log the next 3 quotes** you’re going to finish today.")

    # Phone grade actions
    if phone_grade in ("Below Average", "Poor"):
        actions.append("Use a call list rule: **new leads first**, then follow-ups, then old quotes.")
        actions.append("Do **10 follow-ups** today on quotes already sent (quick win bucket).")

    # Movement grade actions (only if Poor; avoid over-focusing idle)
    if movement_grade == "Poor":
        actions.append("Fix 1 friction point today: identify the **one app/tab** you bounce between the most and keep it pinned + logged in.")
        actions.append("If you get stuck, ask manager for **10 minutes screen-share** to shorten the workflow.")

    # Safety: ensure we always have 10 items
    # If we somehow have less than 10, pad with strong generic-but-direct ops actions.
    while len(actions) < 10:
        actions.append("Before end of day: send **5 follow-up texts/emails** to warm prospects with a clear next step.")

    # -------------------------
    # One-line focus (simple)
    # -------------------------
    if ob_level == "bad" and obt_level == "bad":
        focus = "Focus today: **build real conversations** (protected call blocks + immediate follow-up)."
    elif ep_level == "bad":
        focus = "Focus today: **convert activity into proposals** (follow-up sprints after calls)."
    elif quote_grade in ("Below Average", "Poor"):
        focus = "Focus today: **tight quoting workflow** (2 batches, no stacking)."
    else:
        focus = "Focus today: **repeat consistency** (call blocks + follow-ups)."

    return {
        "well": " ".join(well_parts),
        "improve": " ".join(improve_parts),
        "actions": actions,
        "focus": focus
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

        <h3 style="margin:16px 0 8px 0;">3) Performance Grades (Last 7 days)</h3>

        <table style="width:100%; border-collapse:collapse;">
          <tr style="color:#bffcff;">
            <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Area</th>
            <th style="text-align:right; padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Grade</th>
            <th style="text-align:left; padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">What it means</th>
          </tr>

          <tr>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Phone</td>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:right;">{buckets.get("phone_grade","-")}</td>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">
              { "Strong call activity and conversations." if buckets.get("phone_grade") in ["Excellent","Above Average"]
                else ("Baseline activity — room to push volume and talk time." if buckets.get("phone_grade") == "Average"
                else "Below target — protect a call block and raise attempts." ) }
            </td>
          </tr>

          <tr>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">Quotes</td>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:right;">{buckets.get("quote_grade","-")}</td>
            <td style="padding:8px; border-bottom:1px solid rgba(255,255,255,0.08);">
              { "Strong quote output / follow-through." if buckets.get("quote_grade") in ["Excellent","Above Average"]
                else ("Baseline quoting — tighten workflow to increase output." if buckets.get("quote_grade") == "Average"
                else "Below target — run quoting in 2 short batches so it doesn’t stack." ) }
            </td>
          </tr>

          <tr>
            <td style="padding:8px;">Workflow</td>
            <td style="padding:8px; text-align:right;">{buckets.get("movement_grade","-")}</td>
            <td style="padding:8px;">
              { "Smooth workflow — low friction." if buckets.get("movement_grade") in ["Excellent","Above Average"]
                else ("Normal workflow pace." if buckets.get("movement_grade") == "Average"
                else "High friction — ask for 10 min help to fix one bottleneck." ) }
            </td>
          </tr>
        </table>


        <h3 style="margin:16px 0 8px 0;">4) Coaching</h3>
        <div style="color:rgba(233,246,255,0.85); line-height:1.45;">
          <div style="margin-bottom:10px; padding:10px; background:rgba(0,230,230,0.08); border:1px solid rgba(0,230,230,0.18); border-radius:10px;">
            <b>{coaching["focus"]}</b>
          </div>

          <b>What you did well:</b> {coaching["well"]}<br/><br/>
          <b>What to improve:</b> {coaching["improve"]}<br/><br/>

          <b>Today’s action plan (do these in order):</b>
          <ol style="margin:10px 0 0 18px; padding:0; color:#e9f6ff;">
            {''.join([f"<li style='margin:6px 0;'>{a}</li>" for a in coaching["actions"]])}
          </ol>
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
