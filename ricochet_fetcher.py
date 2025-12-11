# Re-test Ricochet "call-duration" for YESTERDAY.
# Tries BOTH date-window styles that different tenants use:
#  1) from_date = D, to_date = D            (strict same-day)
#  2) from_date = D, to_date = D+1 day      (inclusive/exclusive fix)
# Prints raw + seconds + HH:MM:SS. Saves payloads to JSON for inspection.

import os, sys, re, json
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TENANT = "https://A0D6225.ricochet.me"
URL = f"{TENANT}/api/v4/admin/reports/dashboard_reports/call-duration-report"

COMPANY_TOKEN = os.getenv("RICO_COMPANY_TOKEN")
AUTH_TOKEN    = os.getenv("RICO_AUTH_TOKEN")
if not COMPANY_TOKEN or not AUTH_TOKEN:
    print("ERROR: set RICO_COMPANY_TOKEN and RICO_AUTH_TOKEN first.")
    sys.exit(1)

HEADERS = {
    "Accept": "application/json",
    "X-Company-Token": COMPANY_TOKEN,
    "X-Auth-Token": AUTH_TOKEN,
}

PT = ZoneInfo("America/Los_Angeles")

def ymd(d): return d.strftime("%Y-%m-%d")

def to_secs(val):
    """Parse seconds from values like 123, '38 secs', '00:05:12', or '2 min'."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip().lower()
    # HH:MM:SS
    if re.match(r"^\d{1,2}:\d{2}:\d{2}$", s):
        h, m, sec = map(int, s.split(":"))
        return h * 3600 + m * 60 + sec
    # "2 min", "2 mins", "2 minutes"
    m = re.match(r"^\s*(\d+)\s*m(in(ute)?s?)?\s*$", s)
    if m:
        return int(m.group(1)) * 60
    # "... secs", "90s", plain integer string
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None

def hhmmss(seconds):
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:01d}:{m:02d}:{s:02d}"

def pick_first(obj, keys):
    """Find first matching key (nested) and return the value."""
    want = {re.sub(r"[^a-z0-9]", "", k.lower()) for k in keys}
    def walk(v):
        if isinstance(v, dict):
            for k, vv in v.items():
                if re.sub(r"[^a-z0-9]", "", str(k).lower()) in want:
                    return vv
                got = walk(vv)
                if got is not None:
                    return got
        elif isinstance(v, list):
            for it in v:
                got = walk(it)
                if got is not None:
                    return got
        return None
    return walk(obj)

def run_variant(label, params):
    print(f"\n=== VARIANT: {label} ===")
    try:
        r = requests.get(URL, headers=HEADERS, params=params, timeout=30)
        print(f"GET {URL} {params} -> {r.status_code} {r.headers.get('content-type')}")
        print((r.text or "")[:280], "\n")
        r.raise_for_status()
    except Exception as e:
        print(f"Request failed: {e}")
        return

    j = r.json()
    payload = j.get("data", j)
    # Save for inspection
    out_name = f"ricochet_call_duration_{label}.json".replace(" ", "_")
    with open(out_name, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved {out_name}")

    # Metrics are usually nested under "data", but support both shapes
    metrics = payload.get("data", payload)

    avg_raw = pick_first(metrics, ["average_call_duration","average","avg_duration","avg_time"])
    gt90    = pick_first(metrics, ["percent_of_calls_over_90","over_90","gt90"])
    gt300   = pick_first(metrics, ["percent_of_calls_over_300","over_300","gt300"])
    total   = pick_first(metrics, ["total_time","total_duration","handle_time","talk_time_total"])

    avg_sec   = to_secs(avg_raw)   if avg_raw  is not None else None
    total_sec = to_secs(total)     if total    is not None else None

    print("— Parsed fields —")
    print(f"average_call_duration (raw): {avg_raw!r}")
    if avg_sec is not None:
        print(f"average_call_duration (sec): {avg_sec}  ({hhmmss(avg_sec)})")
    else:
        print("average_call_duration: not parsed")

    if total is not None:
        print(f"total_duration/handle_time (raw): {total!r}")
    if total_sec is not None:
        print(f"total_duration/handle_time (sec): {total_sec}  ({hhmmss(total_sec)})")

    if gt90 is not None:
        print(f">% over 90 sec: {gt90}")
    if gt300 is not None:
        print(f">% over 300 sec: {gt300}")

def main():
    today_pt = datetime.now(tz=PT).date()
    yesterday = today_pt - timedelta(days=1)
    next_day  = yesterday + timedelta(days=1)

    # Try both window styles
    variants = [
        ("strict_same_day",
         {"from_date": ymd(yesterday), "to_date": ymd(yesterday)}),
        ("inclusive_next_day",
         {"from_date": ymd(yesterday), "to_date": ymd(next_day)}),
        # also try start/end synonyms (some tenants accept these)
        ("strict_same_day_start_end",
         {"start": ymd(yesterday), "end": ymd(yesterday)}),
        ("inclusive_next_day_start_end",
         {"start": ymd(yesterday), "end": ymd(next_day)}),
    ]

    print(f"Tenant: A0D6225   Date under test (PT): {yesterday} (yesterday)")
    for label, qs in variants:
        run_variant(label, qs)

if __name__ == "__main__":
    main()
