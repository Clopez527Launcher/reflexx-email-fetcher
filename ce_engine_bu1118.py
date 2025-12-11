import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime

# ---------------------------------
# DB CONFIG (Railway direct access)
# ---------------------------------
MYSQL_CONFIG = {
    "host": "autorack.proxy.rlwy.net",
    "user": "root",
    "password": "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc",
    "database": "railway",
    "port": 55185
}

# --------------------------------------------------
# STEP 1 — Load CE Coefficients for ALL CE Buckets
# --------------------------------------------------
def load_ce_coefficients(conn):
    sql = """
        SELECT metric_name, coefficient
        FROM stats
        WHERE metric_name IN (
            -- PHONE
            'inbounds','outbounds','ib_time_minutes','ob_time_minutes',
            -- QUOTING
            'advisor_pro_minutes','quotes_unique','quoted_items',
            -- MOVEMENT
            'keystrokes','mouse_clicks','mouse_distance','idle_time_seconds'
        )
    """
    df = pd.read_sql(sql, conn)
    coeffs = {row["metric_name"]: float(row["coefficient"]) for _, row in df.iterrows()}
    return coeffs


# ------------------------------------------------------
# STEP 2 — Load last 90 days from fact_daily for ALL CE
# ------------------------------------------------------
def load_fact_daily(conn):
    sql = """
        SELECT
            date, user_id,

            -- PHONE
            inbounds, outbounds,
            ib_time_minutes, ob_time_minutes,

            -- QUOTING
            advisor_pro_minutes,
            quotes_unique,
            quoted_items,

            -- MOVEMENT
            keystrokes,
            mouse_clicks,
            mouse_distance,
            idle_time_seconds

        FROM fact_daily
        WHERE date >= CURDATE() - INTERVAL 90 DAY
        ORDER BY user_id, date
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
# STEP 3B — Compute RAW QUOTING CE
# -----------------------------------------------------
def compute_raw_quote_ce(df, coeffs):
    df["quote_ce_raw"] = (
        df["advisor_pro_minutes"] * coeffs["advisor_pro_minutes"] +
        df["quotes_unique"]       * coeffs["quotes_unique"] +
        df["quoted_items"]        * coeffs["quoted_items"]
    )
    return df


# -----------------------------------------------------
# STEP 3C — Compute RAW MOVEMENT CE
# -----------------------------------------------------
def compute_raw_movement_ce(df, coeffs):
    df["movement_ce_raw"] = (
        df["keystrokes"]        * coeffs["keystrokes"] +
        df["mouse_clicks"]      * coeffs["mouse_clicks"] +
        df["mouse_distance"]    * coeffs["mouse_distance"] +
        df["idle_time_seconds"] * coeffs["idle_time_seconds"]
    )
    return df


# -----------------------------------------------------
# STEP 4 — Rolling Means, STDs, Z-scores for ANY column
# -----------------------------------------------------
def add_rolling_stats(df, column_prefix):
    """
    column_prefix examples:
        'phone_ce'     → phone_ce_raw, phone_ce_l7_mean, ...
        'quote_ce'     → quote_ce_raw, ...
        'movement_ce'  → movement_ce_raw, ...
    """

    # always treat date as timestamp
    df["date"] = pd.to_datetime(df["date"])
    today = datetime.now().date()

    # exclude today (only completed days)
    df = df[df["date"] < pd.Timestamp(today)]

    raw_col = f"{column_prefix}_raw"

    # remove null/zero days for THIS bucket
    df = df[df[raw_col].notnull() & (df[raw_col] != 0)]

    # sort for rolling windows
    df = df.sort_values(["user_id", "date"])

    # Rolling windows: 7, 30, 60
    for window in [7, 30, 60]:
        mean_col = f"{column_prefix}_l{window}_mean"
        std_col  = f"{column_prefix}_l{window}_stdev"
        z_col    = f"{column_prefix}_l{window}_z"

        df[mean_col] = df.groupby("user_id")[raw_col].transform(
            lambda x: x.rolling(window, min_periods=3).mean()
        )

        df[std_col] = df.groupby("user_id")[raw_col].transform(
            lambda x: x.rolling(window, min_periods=3).std()
        )

        df[z_col] = (df[raw_col] - df[mean_col]) / df[std_col]
        df[z_col] = df[z_col].fillna(0)

    return df


# -----------------------------------------------------
# STEP 5 — Write ALL CE columns back to fact_daily
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

    # Convert NaN → None
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
# MAIN ENGINE — Runs all CE Engines in one pass
# -----------------------------------------------------
def run_ce_engine():

    conn = mysql.connector.connect(**MYSQL_CONFIG)

    print("Loading CE coefficients...")
    coeffs = load_ce_coefficients(conn)

    print("Loading fact_daily...")
    df = load_fact_daily(conn)

    print("Computing PHONE CE...")
    df = compute_raw_phone_ce(df, coeffs)

    print("Computing QUOTE CE...")
    df = compute_raw_quote_ce(df, coeffs)

    print("Computing MOVEMENT CE...")
    df = compute_raw_movement_ce(df, coeffs)

    print("Computing rolling z-scores (PHONE)...")
    df = add_rolling_stats(df, "phone_ce")

    print("Computing rolling z-scores (QUOTE)...")
    df = add_rolling_stats(df, "quote_ce")

    print("Computing rolling z-scores (MOVEMENT)...")
    df = add_rolling_stats(df, "movement_ce")

    print("Updating fact_daily...")
    update_fact_daily(conn, df)

    conn.close()
    print("✔ CE Engine Complete")


if __name__ == "__main__":
    run_ce_engine()
