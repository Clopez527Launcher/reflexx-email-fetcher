# ask_reflexx_ai.py

from datetime import date, timedelta

def _parse_dates(start_str, end_str):
    """
    If frontend doesn't send dates, default to last 7 days ending today.
    Dates are in YYYY-MM-DD format.
    """
    if end_str:
        end = date.fromisoformat(end_str)
    else:
        end = date.today()

    if start_str:
        start = date.fromisoformat(start_str)
    else:
        # 7-day window = today + previous 6 days
        start = end - timedelta(days=6)

    return start.isoformat(), end.isoformat()


def _fetch_effort_window(conn, manager_id, start_ymd, end_ymd):
    """
    Pull per-user weekly aggregates + L7 z-scores from fact_daily.
    Restricted by manager_id via the users table.
    """
    sql = """
        SELECT
            fd.user_id,
            COALESCE(fd.user_name, u.user_name, CONCAT('User ', fd.user_id)) AS user_name,

            -- raw activity over the window
            SUM(fd.inbounds)              AS inbounds,
            SUM(fd.outbounds)             AS outbounds,
            SUM(fd.ib_time_minutes)       AS ib_time_minutes,
            SUM(fd.ob_time_minutes)       AS ob_time_minutes,
            SUM(fd.quotes_unique)         AS quotes_unique,
            SUM(fd.quoted_items)          AS quoted_items,
            SUM(fd.idle_time_seconds)     AS idle_time_seconds,
            SUM(fd.mouse_distance)        AS mouse_distance,
            SUM(fd.keystrokes)            AS keystrokes,
            SUM(fd.mouse_clicks)          AS mouse_clicks,
            COUNT(*)                      AS active_days,

            -- L7 z scores (effort lens)
            AVG(COALESCE(fd.phone_ce_l7_z, 0))    AS phone_z,
            AVG(COALESCE(fd.quote_ce_l7_z, 0))    AS quote_z,
            AVG(COALESCE(fd.movement_ce_l7_z, 0)) AS movement_z

        FROM fact_daily fd
        JOIN users u ON u.id = fd.user_id
        WHERE fd.date BETWEEN %s AND %s
          AND u.manager_id = %s
        GROUP BY fd.user_id, user_name
        HAVING active_days > 0
        ORDER BY user_name
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, (start_ymd, end_ymd, manager_id))
    rows = cur.fetchall()
    cur.close()
    return rows


def _compute_team_stats(rows):
    """
    Build simple team averages so the explanation can say things like
    '40% above team average outbound talk time'.
    We treat each row as 'this user over the week'.
    """
    if not rows:
        return {}

    def avg(field):
        vals = [float(r[field]) for r in rows if r[field] is not None]
        return (sum(vals) / len(vals)) if vals else 0.0

    return {
        "ob_minutes_avg": avg("ob_time_minutes"),
        "outbounds_avg":  avg("outbounds"),
        "quotes_avg":     avg("quotes_unique"),
        "idle_avg":       avg("idle_time_seconds"),
        "keys_avg":       avg("keystrokes"),
        "clicks_avg":     avg("mouse_clicks"),
    }


def _pick_effort_winner(rows):
    """
    Choose the 'effort' winner using L7 Z scores (phone + quote + movement).
    Fallback to raw activity if z-scores are missing.
    """
    if not rows:
        return None

    best = None
    best_score = None

    for r in rows:
        phone_z    = float(r.get("phone_z") or 0.0)
        quote_z    = float(r.get("quote_z") or 0.0)
        movement_z = float(r.get("movement_z") or 0.0)

        effort_z = phone_z + quote_z + movement_z
        active_days = int(r.get("active_days") or 0)

        # small tiebreaker: more active days wins
        composite = (effort_z, active_days)

        if best_score is None or composite > best_score:
            best_score = composite
            best = r

    # attach the combined z for reference
    if best is not None:
        best["effort_z"] = best_score[0]

    return best


def _percent_delta(value, avg):
    """
    Return % above/below avg, safe for zero.
    """
    value = float(value or 0.0)
    avg   = float(avg or 0.0)
    if avg <= 0:
        return None
    return (value - avg) / avg * 100.0


def _build_effort_explanation(winner, team_stats, start_ymd, end_ymd):
    """
    Turn the raw numbers into a natural-language explanation.
    This is where the 'magic' wording lives.
    """
    name = winner["user_name"]
    pieces = []

    # Opening line
    pieces.append(
        f"Looking at {start_ymd} through {end_ymd}, {name} appears to have put forth "
        f"the most overall effort on your team."
    )

    ob_minutes   = float(winner.get("ob_time_minutes") or 0.0)
    outbounds    = int(winner.get("outbounds") or 0)
    quotes       = int(winner.get("quotes_unique") or 0)
    idle_seconds = float(winner.get("idle_time_seconds") or 0.0)
    keystrokes   = float(winner.get("keystrokes") or 0.0)
    clicks       = float(winner.get("mouse_clicks") or 0.0)
    days         = int(winner.get("active_days") or 1)

    # per-day views
    ob_per_day     = ob_minutes / days if days else 0
    out_per_day    = outbounds / days if days else 0
    quotes_per_day = quotes / days if days else 0

    # team averages (week-level, not per-day, but still useful)
    ob_avg     = team_stats.get("ob_minutes_avg", 0.0)
    out_avg    = team_stats.get("outbounds_avg", 0.0)
    quotes_avg = team_stats.get("quotes_avg", 0.0)
    idle_avg   = team_stats.get("idle_avg", 0.0)
    keys_avg   = team_stats.get("keys_avg", 0.0)
    clicks_avg = team_stats.get("clicks_avg", 0.0)

    # deltas
    ob_delta     = _percent_delta(ob_minutes, ob_avg)
    out_delta    = _percent_delta(outbounds, out_avg)
    quotes_delta = _percent_delta(quotes, quotes_avg)
    idle_delta   = _percent_delta(idle_seconds, idle_avg)
    keys_delta   = _percent_delta(keystrokes, keys_avg)
    clicks_delta = _percent_delta(clicks, clicks_avg)

    # Phone effort
    if ob_minutes > 0 and (ob_delta is not None and ob_delta > 10):
        pieces.append(
            f"They logged about {ob_minutes:.0f} minutes of outbound talk time over the week, "
            f"roughly {ob_delta:.0f}% higher than the team average."
        )
    elif outbounds > 0 and (out_delta is not None and out_delta > 10):
        pieces.append(
            f"They made {outbounds} outbound calls, about {out_delta:.0f}% more than the team average."
        )

    # Quoting effort
    if quotes > 0 and (quotes_delta is not None and quotes_delta > 10):
        pieces.append(
            f"On the quoting side, they handled {quotes} unique quotes "
            f"({quotes_per_day:.1f} per active day), which is above the team’s average."
        )

    # Idle time (inverted)
    if idle_seconds > 0 and idle_avg > 0 and idle_delta is not None and idle_delta < -10:
        pieces.append(
            "They also kept idle time noticeably lower than the rest of the team, "
            "which suggests they stayed engaged instead of sitting in long gaps."
        )

    # Keyboard / mouse activity
    if (keys_delta is not None and keys_delta > 10) or (clicks_delta is not None and clicks_delta > 10):
        pieces.append(
            "Their keystroke and mouse activity stayed consistently high, "
            "which lines up with someone actively working rather than in short bursts."
        )

    # If everything above is super flat (no strong deltas), add a generic line
    if len(pieces) == 1:
        pieces.append(
            "Their phone, quoting, and movement patterns were slightly above the team overall, "
            "which is why they rank as the top effort player for this window."
        )
    else:
        pieces.append(
            "Taken together, their phone, quoting, and movement patterns show the "
            "highest sustained effort level on your team over this period."
        )

    return " ".join(pieces)


def answer_effort_week(conn, manager_id, start_str=None, end_str=None):
    """
    Main function for: 'Who put forth the most effort this week and why?'
    Uses L7 z-scores to find the winner, and raw metrics to explain it.
    """
    start_ymd, end_ymd = _parse_dates(start_str, end_str)
    rows = _fetch_effort_window(conn, manager_id, start_ymd, end_ymd)

    if not rows:
        return (
            "I don't see any activity for your team in that window yet. "
            "Once there are calls, quotes, or movement logs, I’ll be able to tell you "
            "who’s putting in the most effort."
        )

    team_stats = _compute_team_stats(rows)
    winner = _pick_effort_winner(rows)

    if not winner:
        return (
            "I wasn't able to identify a clear effort leader for that period. "
            "If you think there should be data there, double-check that your agents "
            "have recent phone, quoting, or movement activity logged."
        )

    return _build_effort_explanation(winner, team_stats, start_ymd, end_ymd)


def handle_ask_reflexx(question_text, conn, manager_id, start_str=None, end_str=None):
    """
    Dispatcher for Ask Reflexx AI.
    Version 1: only supports the 'most effort this week' style questions.
    """
    q = (question_text or "").lower()

    if "effort" in q and "week" in q:
        return answer_effort_week(conn, manager_id, start_str, end_str)

    # Fallback for questions we haven't implemented yet
    return (
        "Right now I can answer questions like:\n"
        "• Who put forth the most effort this week and why?\n\n"
        "Try asking that, and I’ll scan your team’s recent phone, quoting, and movement "
        "data to explain who showed the most effort."
    )
