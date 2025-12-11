import os
import pandas as pd

from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import mysql.connector


# =========================================================
# STEP 0) DB CONFIG (PUBLIC HOST FOR LOCAL RUNS)
# =========================================================
# Use your PUBLIC Railway TCP host/port for local runs:
#   host: autorack.proxy.rlwy.net
#   port: 55185
#
# If you ever run this ON Railway later, switch to:
#   host: mysql.railway.internal
#   port: 3306

DB_HOST = os.getenv("DB_HOST", "autorack.proxy.rlwy.net")
DB_PORT = int(os.getenv("DB_PORT", "55185"))
DB_NAME = os.getenv("DB_NAME", "railway")
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc")


# =========================================================
# STEP 1) RAW FEATURES PER BUCKET (from fact_daily)
# =========================================================

# PHONE bucket raw metrics
PHONE_COLS = [
    "inbounds",
    "outbounds",
    "ib_time_minutes",
    "ob_time_minutes"
]

# MOVEMENT bucket raw metrics
MOVEMENT_COLS = [
    "idle_time_seconds",
    "keystrokes",
    "mouse_clicks",
    "mouse_distance"
]

# QUOTING bucket raw metrics  (✅ NO VC!)
QUOTING_COLS = [
    "quotes_unique",
    "quoted_items",
    "advisor_pro_minutes"
]


# =========================================================
# STEP 2) HELPER: Fit ridge regression + return coef series
# =========================================================
def fit_bucket(df, feature_cols, target_col, bucket_name, alpha=1.0):
    work = df.dropna(subset=feature_cols + [target_col]).copy()

    if work.empty:
        print(f"\n⚠️ {bucket_name}: No usable rows after dropping NULLs.")
        return None

    X = work[feature_cols].astype(float)
    y = work[target_col].astype(float)

    model = Pipeline([
        ("scaler", StandardScaler()),   # z-score features
        ("ridge", Ridge(alpha=alpha))   # stabilizes coefficients
    ])

    model.fit(X, y)
    coefs = model.named_steps["ridge"].coef_

    return pd.Series(coefs, index=feature_cols).sort_values(ascending=False)


# =========================================================
# STEP 3) CONNECT TO MYSQL
# =========================================================
conn = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME
)
print("✅ Connected to MySQL")


# =========================================================
# STEP 4) PULL LAST 30 DAYS ONCE
# (we pull targets for each bucket too)
# =========================================================
all_feature_cols = sorted(set(PHONE_COLS + MOVEMENT_COLS + QUOTING_COLS))

sql = f"""
SELECT
  date,
  user_id,

  -- raw features
  {", ".join(all_feature_cols)},

  -- correct bucket targets
  phone_activity_score,
  movement_activity_score,
  quote_activity_score

FROM fact_daily
WHERE date >= CURDATE() - INTERVAL 30 DAY
"""

df = pd.read_sql(sql, conn)

if df.empty:
    raise ValueError("No rows returned for last 30 days. Check data/dates.")

print(f"✅ Pulled {len(df)} rows from last 30 days")


# =========================================================
# STEP 5) FIT EACH BUCKET AGAINST ITS OWN SCORE
# =========================================================

# PHONE → regress onto phone_activity_score
phone_coefs = fit_bucket(df, PHONE_COLS, "phone_activity_score", "PHONE")

# MOVEMENT → regress onto movement_activity_score
# Note: movement score can be NULL on some days; fit_bucket handles dropna.
move_coefs  = fit_bucket(df, MOVEMENT_COLS, "movement_activity_score", "MOVEMENT")

# QUOTING → regress onto quote_activity_score
quote_coefs = fit_bucket(df, QUOTING_COLS, "quote_activity_score", "QUOTING")


# =========================================================
# STEP 6) PRINT RESULTS
# =========================================================
def print_block(name, series):
    print(f"\n{name} CE COEFFICIENTS (Last 30 Days, Ridge)")
    print("-" * 55)
    if series is None:
        print("No coefficients.")
        return
    for k, v in series.items():
        print(f"{k:22s} {v: .6f}")

print_block("PHONE", phone_coefs)
print_block("MOVEMENT", move_coefs)
print_block("QUOTING", quote_coefs)


# =========================================================
# STEP 7) COPY/PASTE OUTPUT
# =========================================================
print("\n\nCOPY/PASTE TABLE FORMAT")
print("------------------------")
print("bucket,metric,ce_weight")

def print_cp(bucket, series):
    if series is None:
        return
    for k, v in series.items():
        print(f"{bucket},{k},{v:.6f}")

print_cp("phone", phone_coefs)
print_cp("movement", move_coefs)
print_cp("quoting", quote_coefs)


conn.close()
print("\n✅ Done.")
