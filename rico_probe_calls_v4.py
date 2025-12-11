import os, requests, json
from datetime import datetime, timedelta, timezone, time

TENANT = "https://A0D6225.ricochet.me"
BASE   = f"{TENANT}/api/v4"

COMPANY_TOKEN = os.getenv("RICO_COMPANY_TOKEN")
AUTH_TOKEN    = os.getenv("RICO_AUTH_TOKEN")
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Company-Token": COMPANY_TOKEN,
    "X-Auth-Token": AUTH_TOKEN,
}

# yesterday 00:00Z -> today 00:00Z
today = datetime.utcnow().date()
start = datetime.combine(today - timedelta(days=1), time.min, tzinfo=timezone.utc)
end   = datetime.combine(today, time.min, tzinfo=timezone.utc)
start_iso = start.isoformat()
end_iso   = end.isoformat()
start_d   = start.date().isoformat()
end_d     = end.date().isoformat()

ENDPOINTS = [
    f"{BASE}/calls/search",
    f"{BASE}/calls/list",
    f"{BASE}/calls/query",
    f"{BASE}/reports/calls",
    f"{BASE}/reports/call-history",
    f"{BASE}/cdr/search",
]

BODIES = [
    {"start": start_iso, "end": end_iso, "page": 1, "per_page": 200},
    {"date_from": start_iso, "date_to": end_iso, "page": 1, "per_page": 200},
    {"created_from": start_iso, "created_to": end_iso, "page": 1, "per_page": 200},
    {"from_date": start_d, "to_date": end_d, "page": 1, "per_page": 200},
    {"date_from": start_d, "date_to": end_d, "page": 1, "per_page": 200},
    {"filters": {"date_from": start_d, "date_to": end_d}, "page": 1, "per_page": 200},
]

def try_post(url, body):
    r = requests.post(url, headers=HEADERS, data=json.dumps(body), timeout=30)
    ct = r.headers.get("content-type","")
    print(f"POST {url} -> {r.status_code} {ct}")
    print(" body sent:", body)
    print(" resp preview:", (r.text or "")[:240], "\n")
    return r

def extract_items(data):
    if isinstance(data, dict):
        for key in ("data","items","calls","records","results","rows"):
            v = data.get(key)
            if isinstance(v, list):
                return v
    if isinstance(data, list):
        return data
    return []

def main():
    if not COMPANY_TOKEN or not AUTH_TOKEN:
        raise SystemExit("Set RICO_COMPANY_TOKEN and RICO_AUTH_TOKEN first.")
    for ep in ENDPOINTS:
        for body in BODIES:
            r = try_post(ep, body)
            if r.status_code == 200 and "json" in (r.headers.get("content-type","").lower()):
                try:
                    data = r.json()
                except Exception:
                    continue
                items = extract_items(data)
                if items:
                    print(f"✅ SUCCESS: {ep} with body {body}")
                    print(f"Items returned: {len(items)}")
                    return
    print("❌ No v4 calls endpoint responded with JSON results. We’ll need the exact path from tenant docs.")

if __name__ == "__main__":
    main()
