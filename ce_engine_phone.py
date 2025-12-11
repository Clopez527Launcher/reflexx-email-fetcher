import mysql.connector
import pandas as pd
import numpy as np
from datetime import date, timedelta

# ---------------------------------------
# DB CONFIG  (modify if needed)
# ---------------------------------------
MYSQL_CONFIG = {
    "host":     "containers-us-west-XXX.railway.app",
    "user":     "root",
    "password": "YOUR_PASSWORD",
    "database": "railway"
}

# ---------------------------------------
# Fetch CE coefficients from stats table
# ---------------------------------------
def load_phone_coeffs(conn):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT metric_name, coefficient
        FROM stats
        WHERE metric_name IN (
            'inbounds',
            'outbounds',
            'ib_time_minutes',
            'ob_time_minutes'
        )
    """)
    rows = cur.fetchall()
    return {row["metric_name"]: float(row["coefficient"]) for row in rows}


# ---------------------------------------
# Load last 60 days for a given user
# ---------------------------------------
def load_user_history(conn, user_id):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT date,
               inbounds,
               outbounds,
               ib_time_minutes,
               ob_time_minutes
        FROM fact_daily
        WHERE user_id = %s
          AND date >= CURDATE() - INTERVAL 60 DAY
          AND date < CURDATE()       -- EXCLUDE TODAY
        ORDER BY date ASC
    """, (user_id,))
    return cur.fetchall()


# ---------------------------------------
# Compute phone_ce_raw using CE weights
# ---------------------------------------
def compute_phone_ce_raw(row, ce):
    if row is None:
        return None

    return (
        (row["inbounds"] or 0)          * ce["inbounds"] +
        (row["outbounds"] or 0)         * ce["outbounds"] +
        (row["ib_time_minutes"] or 0)   * ce["ib_time_minutes"] +
        (row["ob_time_minutes"] or 0)   * ce["ob_time_minutes"]
    )


# ---------------------------------------
# Compute rolling stats (mean, stdev, z)
# ---------------------------------------
def rolling_stats(series, window):
    if len(series) < 2:
        return (None, None, None)

    w = series[-window:]  # take last N values
    mean = float(np.mean(w))
    stdev = float(np.std(w, ddof=1)) if len(w) > 1 else None

    if stdev and stdev > 0:
        z = float((series[-1] - mean) / stdev)
    else:
        z = None

    return (mean, stdev, z)


# ---------------------------------------
# Write results back into fact_daily
# ---------------------------------------
def update_fact_daily(conn, user_id, dt, values):
    cur = conn.cursor()
    cur.execute("""
        UPDATE fact_daily
        SET phone_ce_raw       = %s,
            phone_ce_l7_mean   = %s,
            phone_ce_l7_stdev  = %s,
            phone_ce_l7_z      = %s,
            phone_ce_l30_mean  = %s,
            phone_ce_l30_stdev = %s,
            phone_ce_l30_z     = %s,
            phone_ce_l60_mean  = %s,
            phone_ce_l60_stdev = %s,
            phone_ce_l60_z     = %s
        WHERE user_id = %s AND date = %s
    """, (
        values["raw"],
        values["l7_mean"], values["l7_stdev"], values["l7_z"],
        values["l30_mean"], values["l30_stdev"], values["l30_z"],
        values["l60_mean"], values["l60_stdev"], values["l60_z"],
        user_id, dt
    ))
    conn.commit()


# ---------------------------------------
# Process all users
# ---------------------------------------
def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    ce = load_phone_coeffs(conn)

    print("Loaded CE coefficients:", ce)

    # Get all users with activity in last 60 days
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT user_id
        FROM fact_daily
        WHERE date >= CURDATE() - INTERVAL 60 DAY
          AND date < CURDATE()
    """)
    users = [u[0] for u in cur.fetchall()]
    print("Users:", users)

    for user_id in users:
        print("\nProcessing user:", user_id)

        rows = load_user_history(conn, user_id)
        if not rows:
            print("  No rows. Skipping.")
            continue

        # Compute phone_ce_raw for each day
        ce_scores = []
        dates = []

        for r in rows:
            raw = compute_phone_ce_raw(r, ce)
            ce_scores.append(raw)
            dates.append(r["date"])

        series = pd.Series(ce_scores)

        # For each date, compute rolling stats
        for idx in range(len(series)):
            # We only compute stats for *the day itself*
            dt = dates[idx]

            # Extract history up to this index (skip today handled earlier)
            hist = series.iloc[:idx+1].dropna().tolist()
            if len(hist) == 0:
                continue

            raw = hist[-1]

            # rolling stats
            l7_mean, l7_stdev, l7_z     = rolling_stats(hist, 7)
            l30_mean, l30_stdev, l30_z  = rolling_stats(hist, 30)
            l60_mean, l60_stdev, l60_z  = rolling_stats(hist, 60)

            update_fact_daily(conn, user_id, dt, {
                "raw": raw,
                "l7_mean": l7_mean, "l7_stdev": l7_stdev, "l7_z": l7_z,
                "l30_mean": l30_mean, "l30_stdev": l30_stdev, "l30_z": l30_z,
                "l60_mean": l60_mean, "l60_stdev": l60_stdev, "l60_z": l60_z
            })

            print(f"  Updated {dt} → raw={raw:.3f}")

    conn.close()
    print("\nDONE — Phone CE Engine complete.")


if __name__ == "__main__":
    main()
