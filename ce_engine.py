import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

# ---------------------------------
# DB CONFIG (Railway – same as app.py)
# ---------------------------------
# (You can remove this comment once it's working)

DB_HOST = "mysql.railway.internal"
DB_USER = "root"
DB_PASSWORD = "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc"  # same as app.py / Railway
DB_NAME = "railway"

MYSQL_CONFIG = {
    "host": DB_HOST,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": 3306,  # same port your app uses
}

print("Using MySQL host:", MYSQL_CONFIG["host"])
print("Using MySQL port:", MYSQL_CONFIG["port"])


# --------------------------------------------------
# STEP 1 — Load CE Coefficients
# --------------------------------------------------
def load_ce_coefficients(conn):
    """
    Load CE coefficients using the most recent effective_date for each metric.
    Print out exactly which coefficients are being used.
    """
    sql = """
        SELECT s.metric_name, s.coefficient, s.effective_date
        FROM stats s
        JOIN (
            SELECT 
                metric_name,
                MAX(effective_date) AS max_date
            FROM stats
            WHERE metric_name IN (
                'inbounds','outbounds','ib_time_minutes','ob_time_minutes',
                'advisor_pro_minutes','quotes_unique','quoted_items',
                'keystrokes','mouse_clicks','mouse_distance','idle_time_seconds'
            )
            GROUP BY metric_name
        ) latest
          ON latest.metric_name = s.metric_name
         AND latest.max_date   = s.effective_date
        WHERE s.metric_name IN (
            'inbounds','outbounds','ib_time_minutes','ob_time_minutes',
            'advisor_pro_minutes','quotes_unique','quoted_items',
            'keystrokes','mouse_clicks','mouse_distance','idle_time_seconds'
        )
        ORDER BY s.metric_name;
    """

    df = pd.read_sql(sql, conn)

    # 🔥 PRINT OUT WHICH ROWS ARE BEING USED
    print("\n================ CE COEFFICIENTS LOADED ================")
    for _, row in df.iterrows():
        print(f"{row['metric_name']}: {row['coefficient']}  (effective {row['effective_date']})")
    print("========================================================\n")

    # Return dict for CE engine
    return {row["metric_name"]: float(row["coefficient"]) for _, row in df.iterrows()}


# ------------------------------------------------------
# STEP 2 — Load last 90 days fact_daily + user role
# ------------------------------------------------------
def load_fact_daily(conn):
    sql = """
        SELECT
            fd.date, fd.user_id,
            u.role,

            -- PHONE
            fd.inbounds, fd.outbounds,
            fd.ib_time_minutes, fd.ob_time_minutes,

            -- QUOTING
            fd.advisor_pro_minutes,
            fd.quotes_unique,
            fd.quoted_items,

            -- MOVEMENT
            fd.keystrokes,
            fd.mouse_clicks,
            fd.mouse_distance,
            fd.idle_time_seconds

        FROM fact_daily fd
        JOIN users u ON u.id = fd.user_id
        WHERE fd.date >= CURDATE() - INTERVAL 90 DAY
        ORDER BY fd.user_id, fd.date
    """
    return pd.read_sql(sql, conn)


# -----------------------------------------------------
# STEP 3A — Compute RAW PHONE CE
# -----------------------------------------------------
def compute_raw_phone_ce(df, coeffs):
    df["phone_ce_raw"] = (
        df["inbounds"]        * coeffs["inbounds"] +
        df["outbounds"]       * coeffs["outbounds"] +
        df["ib_time_minutes"] * coeffs["ib_time_minutes"] +
        df["ob_time_minutes"] * coeffs["ob_time_minutes"]
    )
    return df


# -----------------------------------------------------
# STEP 3B — Compute RAW QUOTING CE (NULL if no quote data)
# -----------------------------------------------------
def compute_raw_quote_ce(df, coeffs):
    df["quote_ce_raw"] = (
        df["advisor_pro_minutes"] * coeffs["advisor_pro_minutes"] +
        df["quotes_unique"]       * coeffs["quotes_unique"] +
        df["quoted_items"]        * coeffs["quoted_items"]
    )

    no_quote_mask = (
        df["advisor_pro_minutes"].fillna(0) == 0
    ) & (
        df["quotes_unique"].fillna(0) == 0
    ) & (
        df["quoted_items"].fillna(0) == 0
    )

    df.loc[no_quote_mask, "quote_ce_raw"] = np.nan
    return df


# -----------------------------------------------------
# STEP 3C — Compute RAW MOVEMENT CE (NULL if no movement data)
# -----------------------------------------------------
def compute_raw_movement_ce(df, coeffs):
    df["movement_ce_raw"] = (
        df["keystrokes"]        * coeffs["keystrokes"] +
        df["mouse_clicks"]      * coeffs["mouse_clicks"] +
        df["mouse_distance"]    * coeffs["mouse_distance"] +
        df["idle_time_seconds"] * coeffs["idle_time_seconds"]
    )

    no_move_mask = (
        df["keystrokes"].fillna(0) == 0
    ) & (
        df["mouse_clicks"].fillna(0) == 0
    ) & (
        df["mouse_distance"].fillna(0) == 0
    ) & (
        df["idle_time_seconds"].fillna(0) == 0
    )

    df.loc[no_move_mask, "movement_ce_raw"] = np.nan
    return df


# -----------------------------------------------------
# STEP 4 — Global Active-Day Rolling Z for one bucket
# -----------------------------------------------------
def add_global_active_day_z(df, bucket_prefix):
    """
    For bucket_prefix in {'phone_ce','quote_ce','movement_ce'}:

    Computes for each window 7/30/60:
      - user window avg skipping inactive days
      - global pooled mean (sum / active_days)
      - global weighted std
      - z-score vs global

    Writes:
      {bucket}_l7_mean/stdev/z, l30..., l60...
    """
    raw_col = f"{bucket_prefix}_raw"

    df["date"] = pd.to_datetime(df["date"])

    pst = pytz.timezone("America/Los_Angeles")
    today = datetime.now(pst).date()
    df = df[df["date"] < pd.Timestamp(today)]  # exclude today

    df = df.sort_values(["user_id", "date"]).copy()

    # Active day = raw present and non-zero
    active_col = f"{bucket_prefix}_active"
    df[active_col] = (df[raw_col].notna()) & (df[raw_col] != 0)
    df[active_col] = df[active_col].astype(int)

    for window in [7, 30, 60]:
        sum_col   = f"{bucket_prefix}_l{window}_sum"
        act_col   = f"{bucket_prefix}_l{window}_active_days"
        avg_col   = f"{bucket_prefix}_l{window}_user_avg"

        mean_col  = f"{bucket_prefix}_l{window}_mean"
        std_col   = f"{bucket_prefix}_l{window}_stdev"
        z_col     = f"{bucket_prefix}_l{window}_z"

        # rolling sums per user (calendar rows)
        df[sum_col] = df.groupby("user_id")[raw_col].transform(
            lambda x: x.rolling(window, min_periods=1).sum()
        )

        # rolling active day counts per user
        df[act_col] = df.groupby("user_id")[active_col].transform(
            lambda x: x.rolling(window, min_periods=1).sum()
        )

        # user avg = sum / active_days (skip non-work days)
        df[avg_col] = np.where(df[act_col] > 0, df[sum_col] / df[act_col], 0.0)

        # ---------------------------
        # GLOBAL pooled mean/stdev by date
        # ---------------------------
        users_only = df["role"] == "user"

        global_by_date = df[users_only].groupby("date").agg(
            global_sum=(sum_col, "sum"),
            global_active_days=(act_col, "sum")
        )

        global_by_date["global_mean"] = np.where(
            global_by_date["global_active_days"] > 0,
            global_by_date["global_sum"] / global_by_date["global_active_days"],
            0.0
        )

        # weighted std to match pooled mean
        # weights = active days, values = user averages
        def weighted_std(sub):
            w = sub[act_col].values.astype(float)
            v = sub[avg_col].values.astype(float)
            wsum = w.sum()
            if wsum == 0:
                return 0.0
            mu = (w * v).sum() / wsum
            var = (w * (v - mu) ** 2).sum() / wsum
            return float(np.sqrt(var))

        # Only pass the columns we actually need into the groupby/apply
        global_std = (
            df.loc[users_only]                       # only real users
              .groupby("date")[[act_col, avg_col]]  # only weights + values
              .apply(weighted_std)
        )
        global_by_date["global_std"] = global_std

        global_by_date["global_std"] = global_std

        # map back onto df
        df = df.merge(
            global_by_date[["global_mean", "global_std"]],
            left_on="date",
            right_index=True,
            how="left"
        )

        df[mean_col] = df["global_mean"]
        df[std_col]  = df["global_std"]

        # z-score
        df[z_col] = np.where(
            df[std_col] > 0,
            (df[avg_col] - df[mean_col]) / df[std_col],
            0.0
        )

        # cleanup temp cols for next loop
        df.drop(columns=["global_mean", "global_std"], inplace=True)

    return df


# -----------------------------------------------------
# STEP 5 — Write updated columns back to fact_daily
# -----------------------------------------------------
def update_fact_daily(conn, df):
    cursor = conn.cursor()

    sql = """
        UPDATE fact_daily
        SET
            -- PHONE
            phone_ce_raw = %s,
            phone_ce_l7_mean = %s,
            phone_ce_l7_stdev = %s,
            phone_ce_l7_z = %s,
            phone_ce_l30_mean = %s,
            phone_ce_l30_stdev = %s,
            phone_ce_l30_z = %s,
            phone_ce_l60_mean = %s,
            phone_ce_l60_stdev = %s,
            phone_ce_l60_z = %s,

            -- QUOTING
            quote_ce_raw = %s,
            quote_ce_l7_mean = %s,
            quote_ce_l7_stdev = %s,
            quote_ce_l7_z = %s,
            quote_ce_l30_mean = %s,
            quote_ce_l30_stdev = %s,
            quote_ce_l30_z = %s,
            quote_ce_l60_mean = %s,
            quote_ce_l60_stdev = %s,
            quote_ce_l60_z = %s,

            -- MOVEMENT
            movement_ce_raw = %s,
            movement_ce_l7_mean = %s,
            movement_ce_l7_stdev = %s,
            movement_ce_l7_z = %s,
            movement_ce_l30_mean = %s,
            movement_ce_l30_stdev = %s,
            movement_ce_l30_z = %s,
            movement_ce_l60_mean = %s,
            movement_ce_l60_stdev = %s,
            movement_ce_l60_z = %s

        WHERE date = %s AND user_id = %s
    """

    df = df.replace({np.nan: None})
    rows = df.to_dict("records")

    for row in rows:
        cursor.execute(sql, (
            # PHONE
            row.get("phone_ce_raw"),
            row.get("phone_ce_l7_mean"), row.get("phone_ce_l7_stdev"), row.get("phone_ce_l7_z"),
            row.get("phone_ce_l30_mean"), row.get("phone_ce_l30_stdev"), row.get("phone_ce_l30_z"),
            row.get("phone_ce_l60_mean"), row.get("phone_ce_l60_stdev"), row.get("phone_ce_l60_z"),

            # QUOTING
            row.get("quote_ce_raw"),
            row.get("quote_ce_l7_mean"), row.get("quote_ce_l7_stdev"), row.get("quote_ce_l7_z"),
            row.get("quote_ce_l30_mean"), row.get("quote_ce_l30_stdev"), row.get("quote_ce_l30_z"),
            row.get("quote_ce_l60_mean"), row.get("quote_ce_l60_stdev"), row.get("quote_ce_l60_z"),

            # MOVEMENT
            row.get("movement_ce_raw"),
            row.get("movement_ce_l7_mean"), row.get("movement_ce_l7_stdev"), row.get("movement_ce_l7_z"),
            row.get("movement_ce_l30_mean"), row.get("movement_ce_l30_stdev"), row.get("movement_ce_l30_z"),
            row.get("movement_ce_l60_mean"), row.get("movement_ce_l60_stdev"), row.get("movement_ce_l60_z"),

            row["date"], row["user_id"]
        ))

    conn.commit()
    cursor.close()


# -----------------------------------------------------
# MAIN ENGINE
# -----------------------------------------------------
def run_ce_engine():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    
    cur = conn.cursor()
    cur.execute("SELECT NOW()")
    print("🔥 NEW GLOBAL ACTIVE-DAY CE ENGINE RUNNING @", cur.fetchone()[0], "🔥")
    cur.close()

    print("Loading CE coefficients...")
    coeffs = load_ce_coefficients(conn)

    print("Loading fact_daily...")
    df = load_fact_daily(conn)

    print("Computing PHONE CE raw...")
    df = compute_raw_phone_ce(df, coeffs)

    print("Computing QUOTE CE raw...")
    df = compute_raw_quote_ce(df, coeffs)

    print("Computing MOVEMENT CE raw...")
    df = compute_raw_movement_ce(df, coeffs)

    print("Computing GLOBAL active-day rolling z-scores (PHONE)...")
    df = add_global_active_day_z(df, "phone_ce")

    print("Computing GLOBAL active-day rolling z-scores (QUOTE)...")
    df = add_global_active_day_z(df, "quote_ce")

    print("Computing GLOBAL active-day rolling z-scores (MOVEMENT)...")
    df = add_global_active_day_z(df, "movement_ce")

    print("Updating fact_daily...")
    update_fact_daily(conn, df)

    conn.close()
    print("✔ CE Engine Complete")


if __name__ == "__main__":
    run_ce_engine()
