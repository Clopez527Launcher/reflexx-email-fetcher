import re
import os
from typing import IO

import pandas as pd
import mysql.connector
from mysql.connector import MySQLConnection


# -------------------------------
# Normalizers / parsers
# -------------------------------

def _clean_sub_producer(raw: str) -> str:
    """
    Match DB normalization:
      - strip leading digits + spaces + dash: "354 - Name" -> "Name"
      - remove dots/commas
      - collapse double spaces
      - trim
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    s = re.sub(r"^[\s]*\d+[\s]*-[\s]*", "", s)  # kill "123 - "
    s = s.replace(".", "").replace(",", "")
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


def _to_float(s, default=0.0):
    try:
        return float(str(s).replace(",", "").replace("$", "").strip())
    except Exception:
        return default


def detect_header_row(df_raw: pd.DataFrame) -> int:
    """
    Find the header within first 40 rows by detecting:
      - "Sub Producer"
      - "Quote"
      - "Production Date"
    """
    max_scan = min(40, len(df_raw))
    needles = [("sub", "producer"), ("quote",), ("production", "date")]
    for i in range(max_scan):
        row = df_raw.iloc[i].astype(str).fillna("").str.lower().tolist()
        ok = True
        for group in needles:
            if not any(all(g in cell for g in group) for cell in row):
                ok = False
                break
        if ok:
            return i
    return 0  # fallback to top


def parse_quotes_excel(file_obj: IO) -> pd.DataFrame:
    """
    Returns a DataFrame with EXACT columns (for DB insert):
      agent_number, sub_producer, quote_control_number, production_date,
      product, quoted_item_count, quoted_premium, channel, quote_audit_name
    """
    # Read raw to detect header row
    df_raw = pd.read_excel(file_obj, sheet_name=0, header=None)
    header_idx = detect_header_row(df_raw)

    # Rewind and read with that header
    file_obj.seek(0)
    df = pd.read_excel(file_obj, sheet_name=0, header=header_idx)

    # Column resolver (more forgiving)
    raw_cols = list(df.columns)
    cols = {str(c).strip().lower(): c for c in raw_cols}

    def pick(*candidates):
        """
        Try to find a column by:
          1) exact normalized name
          2) normalized name containing the candidate string
        """
        # exact
        for cand in candidates:
            cand_norm = cand.strip().lower()
            if cand_norm in cols:
                return cols[cand_norm]

        # contains / substring fallback
        for cand in candidates:
            cand_norm = cand.strip().lower()
            for norm_name, orig_name in cols.items():
                if cand_norm in norm_name:
                    return orig_name

        return None

    # Try multiple reasonable variants for each field
    col_sub     = pick("sub producer", "sub-producer", "sub_producer", "producer")
    col_date    = pick("production date", "production_date", "prod date", "quote date")
    col_items   = pick("quoted item count", "quoted items", "item count", "items")
    col_premium = pick("quoted premium($)", "quoted premium", "premium", "quoted_premium")
    col_agent   = pick("agent number", "agent #", "agent", "agent_no")
    col_quote   = pick("quote control number", "quote #", "quote number", "quote_ctrl", "quote")
    col_product = pick("product", "line of business")
    col_channel = pick("channel", "source")


    # Build normalized frame
    out = pd.DataFrame()
    out["sub_producer"]         = df[col_sub] if col_sub else ""
    out["quote_audit_name"]     = out["sub_producer"].apply(_clean_sub_producer)
    out["production_date"]      = pd.to_datetime(df[col_date], errors="coerce").dt.date if col_date else pd.NaT
    out["quoted_item_count"]    = pd.to_numeric(df[col_items], errors="coerce").fillna(0).astype(int) if col_items else 0
    out["quoted_premium"]       = df[col_premium].apply(_to_float) if col_premium else 0.0
    out["agent_number"]         = df[col_agent].astype(str).str.strip() if col_agent else ""
    out["quote_control_number"] = df[col_quote].astype(str).str.strip() if col_quote else ""
    out["product"]              = df[col_product].astype(str).str.strip() if col_product else ""
    out["channel"]              = df[col_channel].astype(str).str.strip() if col_channel else ""

    # Filter valid rows
    out = out[(out["production_date"].notna()) & (out["quote_audit_name"].astype(str).str.strip() != "")]
    # Exact order expected by insert
    return out[[
        "agent_number",
        "sub_producer",
        "quote_control_number",
        "production_date",
        "product",
        "quoted_item_count",
        "quoted_premium",
        "channel",
        "quote_audit_name",
    ]].copy()


# -------------------------------
# DB connection (Railway-ready)
# -------------------------------

def _connect_from_env() -> MySQLConnection:
    """
    Creates a MySQL connection from common Railway env vars.
    Supports either a single DATABASE_URL or separate MYSQL* vars.
    """
    url = os.environ.get("DATABASE_URL") or os.environ.get("MYSQL_URL") or os.environ.get("JAWSDB_URL")
    if url and url.startswith("mysql"):
        # Example: mysql://user:pass@host:3306/dbname
        import urllib.parse as up
        parsed = up.urlparse(url)
        return mysql.connector.connect(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=parsed.username,
            password=parsed.password,
            database=parsed.path.lstrip("/"),
            charset="utf8mb4",
            use_unicode=True,
        )

    host = os.environ.get("MYSQLHOST") or os.environ.get("MYSQL_HOST") or "localhost"
    port = int(os.environ.get("MYSQLPORT") or os.environ.get("MYSQL_PORT") or 3306)
    user = os.environ.get("MYSQLUSER") or os.environ.get("MYSQL_USER") or "root"
    password = os.environ.get("MYSQLPASSWORD") or os.environ.get("MYSQL_PASSWORD") or ""
    database = os.environ.get("MYSQLDATABASE") or os.environ.get("MYSQL_DATABASE") or "railway"

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        use_unicode=True,
    )


# -------------------------------
# Inserts
# -------------------------------

def insert_into_quotes_raw_rows(
    df: pd.DataFrame,
    report_id: int,
    conn: MySQLConnection | None = None
) -> int:
    """
    Insert parsed quotes into quotes_raw_rows.

    Expects df columns EXACTLY like parse_quotes_excel returns:
      agent_number, sub_producer, quote_control_number, production_date,
      product, quoted_item_count, quoted_premium, channel, quote_audit_name
    """
    opened_here = False
    if conn is None:
        conn = _connect_from_env()
        opened_here = True

    cur = conn.cursor()

    insert_sql = """
        INSERT INTO quotes_raw_rows (
            uploaded_report_id,
            agent_number,
           sub_producer,
            quote_control_number,
            production_date,
            product,
            quoted_item_count,
            quoted_premium,
            channel,
            quote_audit_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            -- if this quote_control_number already exists, refresh it
            uploaded_report_id = VALUES(uploaded_report_id),
            agent_number       = VALUES(agent_number),
            sub_producer       = VALUES(sub_producer),
            production_date    = VALUES(production_date),
            product            = VALUES(product),
            quoted_item_count  = VALUES(quoted_item_count),
            quoted_premium     = VALUES(quoted_premium),
            channel            = VALUES(channel),
            quote_audit_name   = VALUES(quote_audit_name)
    """

    # pandas NaT/NaN -> None
    def _none(v):
        try:
            return None if pd.isna(v) else v
        except Exception:
            return v

    rows = []
    for _, r in df.iterrows():
        # Skip obviously empty lines
        if not (str(r.get("agent_number", "")).strip() or
                str(r.get("quote_control_number", "")).strip() or
                str(r.get("sub_producer", "")).strip()):
            continue

        rows.append((
            report_id,
            str(r.get("agent_number", "") or "").strip(),
            str(r.get("sub_producer", "") or "").strip(),
            str(r.get("quote_control_number", "") or "").strip(),
            _none(r.get("production_date")),                         # date or None
            str(r.get("product", "") or "").strip(),
            int(r.get("quoted_item_count", 0) or 0),
            float(r.get("quoted_premium", 0) or 0.0),
            str(r.get("channel", "") or "").strip(),
            str(r.get("quote_audit_name", "") or "").strip(),        # cleaned name
        ))

    if not rows:
        if opened_here:
            cur.close(); conn.close()
        print("No rows to insert.")
        return 0

    cur.executemany(insert_sql, rows)
    conn.commit()
    inserted = cur.rowcount
    cur.close()
    if opened_here:
        conn.close()

    print(f"Inserted {inserted} rows into quotes_raw_rows.")
    return inserted
    
def import_quotes_from_path(file_path: str, report_id: int, conn: MySQLConnection | None = None) -> int:
    """
    Helper for when the file is already saved on disk.
    1) open the xlsx
    2) parse it with parse_quotes_excel
    3) insert rows into quotes_raw_rows with the given report_id
    """
    with open(file_path, "rb") as f:
        df = parse_quotes_excel(f)

    # if nothing parsed, just stop
    if df is None or df.empty:
        print("No rows parsed from", file_path)
        return 0

    # reuse existing insert logic
    return insert_into_quotes_raw_rows(df, report_id, conn)
    
def aggregate_by_user_day(df: pd.DataFrame, name_to_user_id: dict, manager_id: int | None):
    """
    Back-compat helper for older code paths.
    Works with parse_quotes_excel() output columns:
      - quote_audit_name (cleaned)
      - production_date  (date)
      - quoted_item_count (int)
      - quoted_premium    (float)

    Returns a list of dicts with:
      manager_id, user_id, day, quotes_count, quoted_items, quoted_premium
    """
    if df is None or df.empty:
        return []

    # Map rows to user_id using a case-insensitive lookup
    rows = []
    for _, r in df.iterrows():
        name = str(r.get("quote_audit_name", "") or "").strip().lower()
        user_id = name_to_user_id.get(name)
        if not user_id:
            continue

        rows.append({
            "manager_id": manager_id,
            "user_id": user_id,
            "day": r.get("production_date"),
            "quoted_items": int(r.get("quoted_item_count", 0) or 0),
            "quoted_premium": float(r.get("quoted_premium", 0) or 0.0),
        })

    if not rows:
        return []

    tmp = pd.DataFrame(rows)
    grouped = tmp.groupby(
        ["manager_id", "user_id", "day"], as_index=False
    ).agg(
        quotes_count=("user_id", "count"),
        quoted_items=("quoted_items", "sum"),
        quoted_premium=("quoted_premium", "sum"),
    )

    return grouped.to_dict(orient="records")


