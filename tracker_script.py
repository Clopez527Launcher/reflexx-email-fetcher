import os
import sys
import time
import requests
import platform
import ctypes
import threading
import tkinter as tk
from tkinter import messagebox
from datetime import datetime
import pytz
from PIL import Image, ImageTk

# ===== Sales calendar + MySQL (add below PIL imports) =====
import decimal
try:
    from tkcalendar import Calendar  # optional; falls back to text entry if missing
    _HAS_TKCAL = True
except Exception:
    _HAS_TKCAL = False

import mysql.connector


def detect_label_from_title(title: str) -> str:
    title = title.lower()

    # Ricochet / SpeedToContact
    # examples: "ricochet | speedtocontact", "speed to contact - ricochet"
    if ("ricochet" in title) or ("speedtocontact" in title) or ("speed to contact" in title):
        return "Ricochet"

    if "ringcentral" in title:
        return "RingCentral"
    elif "advisor pro" in title:
        return "Allstate Advisor Pro"
    elif "policy view" in title:
        return "Policy View 2.0"
    elif "quick quote" in title:
        return "California Fair Plan"
    elif "aegis" in title:
        return "Aegis Insurance"
    elif "gateway" in title:
        return "Allstate Gateway"
    elif "eagent" in title:
        return "eAgent"
    elif "policy list" in title or "policies >" in title:
        return "Bamboo Insurance"
    elif "outlook" in title:
        return "Outlook"
    else:
        return "Other"

def resource_path(name: str) -> str:
    base = getattr(sys, "_MEIPASS", os.path.abspath("."))
    return os.path.join(base, name)

# ✅ Headless environment check (Railway)
IS_HEADLESS = os.getenv("RAILWAY_ENVIRONMENT") is not None

if IS_HEADLESS:
    print("⚠️ Headless environment detected. Exiting tracker_script.py.")
    sys.exit(0)

# ---------- Safe Windows paths (works for user, task, or service) ----------
APPDATA = os.getenv("APPDATA") or os.getenv("PROGRAMDATA") or os.path.expanduser(r"~\AppData\Roaming")
REFLEXX_DIR = os.path.join(APPDATA, "Reflexx")
os.makedirs(REFLEXX_DIR, exist_ok=True)  # ensure the folder exists

lock_file        = os.path.join(REFLEXX_DIR, "reflexx.lock")
CACHE_PATH       = os.path.join(REFLEXX_DIR, "agency_hours.json")
CREDENTIALS_PATH = os.path.join(REFLEXX_DIR, "credentials.json")

print(f"[paths] REFLEXX_DIR={REFLEXX_DIR}")


import signal
from ctypes import wintypes

# ----- robust PID check on Windows -----
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
STILL_ACTIVE = 259
OpenProcess = ctypes.windll.kernel32.OpenProcess
OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
OpenProcess.restype = wintypes.HANDLE
GetExitCodeProcess = ctypes.windll.kernel32.GetExitCodeProcess
GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
GetExitCodeProcess.restype = wintypes.BOOL
CloseHandle = ctypes.windll.kernel32.CloseHandle

def _pid_running_windows(pid: int) -> bool:
    try:
        h = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not h:
            return False
        try:
            code = wintypes.DWORD()
            if not GetExitCodeProcess(h, ctypes.byref(code)):
                return False
            return code.value == STILL_ACTIVE
        finally:
            CloseHandle(h)
    except Exception:
        return False

def _read_lock_pid(path: str) -> int | None:
    try:
        with open(path, "r") as f:
            return int((f.read() or "").strip())
    except Exception:
        return None

def acquire_lock_or_exit() -> bool:
    """
    Returns True if we acquired the instance lock.
    If another live instance holds it, return False.
    If the lock is stale, we remove it and acquire it.
    """
    try:
        os.makedirs(os.path.dirname(lock_file), exist_ok=True)
    except Exception:
        pass

    if os.path.exists(lock_file):
        pid = _read_lock_pid(lock_file)
        if pid and _pid_running_windows(pid):
            print(f"🔒 Another Reflexx instance is running (PID {pid}).")
            return False
        else:
            print("🧹 Removing stale lock…")
            try:
                os.remove(lock_file)
            except Exception:
                pass

    # create/overwrite our lock with our PID
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))

    # remove on exit & signals
    atexit.register(remove_lock)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (remove_lock(), sys.exit(0)))
    # (Windows has no SIGKILL; SIGBREAK optional)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: (remove_lock(), sys.exit(0)))

    print(f"✅ Acquired lock for PID {os.getpid()}")
    return True

# ✅ Delete lock on clean exit
import atexit
def remove_lock():
    if os.path.exists(lock_file):
        os.remove(lock_file)

# ✅ Prevent launch outside work hours
def within_work_hours():
    """Return True if current Pacific time is between work_start and work_end (inclusive of start, exclusive of end)."""
    pacific = pytz.timezone("US/Pacific")
    now_local = datetime.now(pacific).time()

    hours = get_cached_agency_hours()
    start = datetime.strptime(hours.get("work_start", "09:00:00"), "%H:%M:%S").time()
    end   = datetime.strptime(hours.get("work_end",   "17:30:00"), "%H:%M:%S").time()

    # 🔎 Debug: see exactly which hours are used
    print(f"[within_work_hours] Using start={start}, end={end}, now={now_local}")

    return start <= now_local < end

# ✅ Import necessary libraries for tracking
if not IS_HEADLESS:
    if platform.system() == "Windows":
        import mouse
        import win32gui  # For active window title tracking
    import keyboard
    import pyautogui

# 🚀 Server API Endpoints
# ===== MySQL config for direct writes to sales_daily (HARD-CODED so password is sent) =====
MYSQL_CONFIG = {
    "host": "autorack.proxy.rlwy.net",
    "user": "root",
    "password": "vbNVbSKVuUvYRJzhewpufAXbxcatfKIc",   # <- type the actual password you use at the mysql CLI
    "database": "railway",
    "port": 55185,                         # <- your Railway proxy port from the CLI command
    "auth_plugin": "mysql_native_password" # helps on some hosts; safe to include
}


def _db_conn():
    return mysql.connector.connect(**MYSQL_CONFIG)

SERVER_URL = "https://reflexxapp.up.railway.app"
AUTH_API = f"{SERVER_URL}/api/authenticate"
TRACK_API = f"{SERVER_URL}/log_activity"

# ✅ Tracking Variables
mouse_distance = 0
keystrokes = 0
mouse_clicks = 0
idle_count = 0
tracking = False
USER_ID = None  # User ID retrieved after authentication
session_start_time = None  # To record when the page/session starts

KEYBOARD_HOOK = None
MOUSE_CLICK_HANDLER = None

from collections import defaultdict

# Dictionary to hold time (in seconds) spent on each tracked page
page_time = defaultdict(int)
page_time["Other"] = 0  # Optional, but clear

# ✅ Utility: Get UTC Timestamp (remains unchanged)
from datetime import timezone  # <-- put near your other imports if not present

def get_utc_timestamp():
    # timezone-aware UTC datetime (no deprecation warning)
    return datetime.now(timezone.utc).isoformat()
    
def get_pacific_date_string():
    pacific = pytz.timezone("US/Pacific")
    now_local = datetime.now(pacific)
    return now_local.strftime("%Y-%m-%d")    

# ✅ Cache close time for agency

REFLEXX_API = "https://reflexxapp.up.railway.app"

RUNNING = True

# ── Keep Windows awake while tracking ──────────────────────────────────────────
KEEP_DISPLAY_ON = False   # set True if you also want to keep the monitor from turning off
keep_awake_thread = None

def _set_keep_awake(on: bool):
    """Tell Windows to stay awake while this thread is alive."""
    if platform.system() != "Windows":
        return
    ES_CONTINUOUS       = 0x80000000
    ES_SYSTEM_REQUIRED  = 0x00000001
    ES_DISPLAY_REQUIRED = 0x00000002

    flags = ES_CONTINUOUS
    if on:
        flags |= ES_SYSTEM_REQUIRED
        if KEEP_DISPLAY_ON:
            flags |= ES_DISPLAY_REQUIRED

    # Returns previous state (not used), but calling with ES_CONTINUOUS makes it stick
    ctypes.windll.kernel32.SetThreadExecutionState(flags)

def _stay_awake_loop():
    """Refresh the awake request periodically (expires if not updated)."""
    while tracking and RUNNING:
        _set_keep_awake(True)
        time.sleep(60)           # refresh once a minute
    _set_keep_awake(False)       # clear request when tracking stops
# ───────────────────────────────────────────────────────────────────────────────


def shutdown_if_past_cutoff():
    global RUNNING
    while True:
        if not within_work_hours():
            print("🛑 Work hours ended. Shutting down Reflexx.")
            remove_lock()
            RUNNING = False          # stop main loop
            stop_tracking()          # ⛔ unhook keyboard/mouse + stop worker loops
            break
        time.sleep(60)

def fetch_and_cache_agency_hours(user_id):
    try:
        response = requests.get(f"{REFLEXX_API}/api/get-agency-hours/{user_id}", timeout=10)
        if response.status_code == 200:
            data = response.json()

            # Ensure directory exists
            os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)

            with open(CACHE_PATH, "w") as f:
                json.dump(data, f)

            print(f"✅ Cached agency hours: {data}")
            return data
        else:
            print("⚠️ Failed to fetch agency hours. Using fallback.")
    except Exception as e:
        print(f"❌ Exception fetching agency hours: {e}")

    return {
        "work_start": "09:00:00",
        "work_end": "17:30:00"
    }

def get_cached_agency_hours():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r") as f:
                return json.load(f)
        except:
            pass
    return {
        "work_start": "09:00:00",
        "work_end": "17:30:00"
    }

def hourly_check_for_update():
    while True:
        pacific = pytz.timezone("US/Pacific")
        now = datetime.now(pacific)
        if now.hour == 10 and now.minute == 0:
            fetch_and_cache_agency_hours(USER_ID)
        time.sleep(60)  # check once a minute    

print("🔎 Time check:", time.time())
print("🕒 Pacific Time:", datetime.now(pytz.timezone("US/Pacific")).time())
print("📁 Cache path:", CACHE_PATH)

# 🔒 Background threads are started lazily (after successful login)
shutdown_thread = None
update_thread = None

def ensure_background_threads():
    """Start cutoff watcher + hourly cache refresher once."""
    global shutdown_thread, update_thread
    if shutdown_thread is None or not shutdown_thread.is_alive():
        shutdown_thread = threading.Thread(target=shutdown_if_past_cutoff, daemon=True)
        shutdown_thread.start()
    if update_thread is None or not update_thread.is_alive():
        update_thread = threading.Thread(target=hourly_check_for_update, daemon=True)
        update_thread.start()


# ✅ Mouse Movement Tracking
def track_mouse():
    global mouse_distance
    prev_x, prev_y = pyautogui.position() if not IS_HEADLESS else (0, 0)

    while tracking and RUNNING:
        try:
            if not IS_HEADLESS:
                x, y = pyautogui.position()
                mouse_distance += ((x - prev_x) ** 2 + (y - prev_y) ** 2) ** 0.5
                prev_x, prev_y = x, y
        except Exception as e:
            print(f"❌ Mouse tracking error: {e}")
        time.sleep(0.1)


# ✅ Idle Time Detection
def get_idle_time():
    if platform.system() == "Windows":
        class LASTINPUTINFO(ctypes.Structure):
            _fields_ = [("cbSize", ctypes.c_uint), ("dwTime", ctypes.c_uint)]
        lii = LASTINPUTINFO()
        lii.cbSize = ctypes.sizeof(LASTINPUTINFO)
        ctypes.windll.user32.GetLastInputInfo(ctypes.byref(lii))
        millis = ctypes.windll.kernel32.GetTickCount() - lii.dwTime
        return millis / 1000.0
    return 0

def check_idle():
    global idle_count
    last_recorded_idle = 0
    baseline_set = False

    while tracking and RUNNING:
        time.sleep(1)
        current_idle = get_idle_time()

        # 🛡️ NEW: set baseline on first loop so we don’t backfill pre-launch idle
        if not baseline_set:
            last_recorded_idle = current_idle
            baseline_set = True
            continue

        if current_idle > 300:
            # Only count idle beyond whichever is larger (300s threshold or last sample)
            increment = current_idle - max(last_recorded_idle, 300)
            if increment > 0:
                idle_count += increment
        # Always update baseline
        last_recorded_idle = current_idle

# ✅ Track Keystrokes
def track_keystrokes():
    global keystrokes, KEYBOARD_HOOK

    # Keep a reference to the callback (keyboard.unhook needs the same function object)
    def on_press(event):
        global keystrokes
        keystrokes += 1

    KEYBOARD_HOOK = on_press
    keyboard.on_press(KEYBOARD_HOOK)

    try:
        while tracking and RUNNING:
            time.sleep(0.2)
    finally:
        try:
            if KEYBOARD_HOOK:
                keyboard.unhook(KEYBOARD_HOOK)
        except Exception:
            pass
        KEYBOARD_HOOK = None

# ✅ Correct Mouse Click Tracking
def track_mouse_clicks():
    global mouse_clicks, MOUSE_CLICK_HANDLER

    def on_click(event):
        global mouse_clicks
        try:
            import mouse as _mouse
            if isinstance(event, _mouse.ButtonEvent) and event.event_type == "down":
                mouse_clicks += 1
        except Exception:
            # Fallback if mouse library version differs
            mouse_clicks += 1

    MOUSE_CLICK_HANDLER = on_click
    mouse.hook(MOUSE_CLICK_HANDLER)
    try:
        while tracking and RUNNING:
            time.sleep(0.2)
    finally:
        try:
            if MOUSE_CLICK_HANDLER:
                mouse.unhook(MOUSE_CLICK_HANDLER)
        except Exception:
            pass
        MOUSE_CLICK_HANDLER = None


# ✅ Track Active Page Time (Windows Only)
def track_active_page():
    global page_time
    while tracking and RUNNING:
        try:
            active_window = win32gui.GetForegroundWindow()
            title = win32gui.GetWindowText(active_window)
        except Exception as e:
            print(f"❌ Active window tracking error: {e}")
            title = ""

        label = detect_label_from_title(title)
        page_time[label] = page_time.get(label, 0) + 1
        print(f"⏱️ Tracking {label}: {page_time[label]} sec (Title: '{title}')")
        time.sleep(1)

# ===== Save to sales_daily (SQL-only) =====
def save_sales_daily_sql(user_id: int, sale_date: str,
                         vc_policies: int, vc_items: int, vc_premium: float,
                         nonvc_policies: int, nonvc_items: int, nonvc_premium: float):
    """
    SET totals for the given (user_id, sale_date).
    Uses ON DUPLICATE KEY UPDATE to overwrite the day with entered totals.
    """
    conn = _db_conn()
    cur = conn.cursor()
    sql = """
    INSERT INTO sales_daily (
        user_id, sale_date,
        vc_policies, vc_items, vc_premium,
        nonvc_policies, nonvc_items, nonvc_premium
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        vc_policies    = VALUES(vc_policies),
        vc_items       = VALUES(vc_items),
        vc_premium     = VALUES(vc_premium),
        nonvc_policies = VALUES(nonvc_policies),
        nonvc_items    = VALUES(nonvc_items),
        nonvc_premium  = VALUES(nonvc_premium),
        updated_at     = CURRENT_TIMESTAMP
    """
    vals = (user_id, sale_date,
            int(vc_policies), int(vc_items), float(vc_premium or 0),
            int(nonvc_policies), int(nonvc_items), float(nonvc_premium or 0))
    cur.execute(sql, vals)
    conn.commit()
    cur.close(); conn.close()
  
# Read one day from sales_daily (returns dict or None)
def read_sales_daily_sql(user_id: int, sale_date: str):
    conn = _db_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT vc_policies, vc_items, vc_premium,
               nonvc_policies, nonvc_items, nonvc_premium
        FROM sales_daily
        WHERE user_id=%s AND sale_date=%s
        LIMIT 1
    """, (user_id, sale_date))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row  # may be None  
  
# ===== Calendar modal to input sales =====
def open_sales_calendar(user_id: int):
    """
    Pop-up to pick a date and enter totals; saves directly to MySQL.sales_daily.
    """
    import tkinter as tk
    from tkinter import ttk, messagebox
    from datetime import date

    root = tk._default_root or tk.Tk()
    if not tk._default_root:
        root.withdraw()

    win = tk.Toplevel(root)
    win.title("Input Sales")
    win.resizable(False, False)
    try:
        ico_path = resource_path("Reflexx_Icon.ico")
        if os.path.exists(ico_path):
            win.iconbitmap(ico_path)
    except Exception:
        pass

    outer = ttk.Frame(win, padding=10)
    outer.grid(row=0, column=0)

    # left: calendar or text date
    ttk.Label(outer, text="Select Date", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w")
    if _HAS_TKCAL:
        cal = Calendar(outer, selectmode="day", date_pattern="yyyy-mm-dd")
        # auto-load when user clicks a new date
        cal.bind("<<CalendarSelected>>", lambda _e: populate_from_db(get_date()))
        cal.selection_set(date.today())
        cal.grid(row=1, column=0, padx=(0,10), pady=(4,10))
        def get_date(): return cal.get_date()
    else:
        date_var = tk.StringVar(value=date.today().isoformat())
        ttk.Entry(outer, textvariable=date_var, width=12, justify="center").grid(row=1, column=0, padx=(0,10), pady=(4,0))
        ttk.Label(outer, text="yyyy-mm-dd").grid(row=2, column=0, sticky="w")
        def get_date(): return date_var.get().strip()

    # right: numeric inputs
    grid = ttk.Frame(outer)
    grid.grid(row=1, column=1, rowspan=2, sticky="n", padx=(10,0))

    def _row(label, r, init="0"):
        ttk.Label(grid, text=label, width=18, anchor="w").grid(row=r, column=0, sticky="w")
        sv = tk.StringVar(value=init)
        e = ttk.Entry(grid, textvariable=sv, width=14, justify="right")
        e.grid(row=r, column=1, sticky="e", pady=2)
        return sv

    vc_policies_sv    = _row("VC Policies",          0, "0")
    vc_items_sv       = _row("VC Items",             1, "0")
    vc_premium_sv     = _row("VC Premium ($)",       2, "0")
    nonvc_policies_sv = _row("Non-VC Policies",      3, "0")
    nonvc_items_sv    = _row("Non-VC Items",         4, "0")
    nonvc_premium_sv  = _row("Non-VC Premium ($)",   5, "0")
    # place the warning directly under the Non-VC Premium field
    tk.Label(
        grid,
        text="*DO NOT include $ signs",
        fg="#e11d48",
        font=("Segoe UI", 9, "italic")
    ).grid(row=6, column=1, sticky="w", pady=(2, 0))


    def populate_from_db(day_str: str):
        """Fill the six fields from DB for the chosen date; use zeros if none."""
        try:
            row = read_sales_daily_sql(user_id, day_str) or {}
            vc_policies_sv.set(str(row.get("vc_policies", 0)))
            vc_items_sv.set(str(row.get("vc_items", 0)))
            # keep decimals as plain numbers; coerce None to 0
            vc_premium_sv.set(str(row.get("vc_premium", 0)))
            nonvc_policies_sv.set(str(row.get("nonvc_policies", 0)))
            nonvc_items_sv.set(str(row.get("nonvc_items", 0)))
            nonvc_premium_sv.set(str(row.get("nonvc_premium", 0)))
            status.config(text=f"Loaded totals for {day_str}", foreground="#22c55e")
        except Exception as e:
            status.config(text=f"Load error: {e}", foreground="#e11d48")

    status = ttk.Label(outer, text="", foreground="#888")
    status.grid(row=3, column=0, columnspan=2, sticky="w", pady=(6,0))

    # when the window opens, load the currently selected date
    try:
        populate_from_db(get_date())
    except Exception as _e:
        pass

    def _to_num(s):
        s = (s or "").strip()
        if s == "": return 0
        try:
            return float(s)
        except:
            return 0

    def on_save():
        d = get_date()
        try:
            save_sales_daily_sql(
                user_id=user_id, sale_date=d,
                vc_policies=int(_to_num(vc_policies_sv.get())),
                vc_items=int(_to_num(vc_items_sv.get())),
                vc_premium=_to_num(vc_premium_sv.get()),
                nonvc_policies=int(_to_num(nonvc_policies_sv.get())),
                nonvc_items=int(_to_num(nonvc_items_sv.get())),
                nonvc_premium=_to_num(nonvc_premium_sv.get()),
            )
            messagebox.showinfo("Saved", f"Sales saved for {d}.")
            win.destroy()
        except Exception as e:
            status.config(text=f"Error: {e}", foreground="#e11d48")

    btns = ttk.Frame(outer)
    btns.grid(row=4, column=0, columnspan=2, sticky="e", pady=(10,0))
    ttk.Button(btns, text="Save", command=on_save).grid(row=0, column=0, padx=6)
    ttk.Button(btns, text="Cancel", command=win.destroy).grid(row=0, column=1)    


# ✅ Send Data to Flask Server (with Debugging)
def send_data():
    global mouse_distance, keystrokes, mouse_clicks, idle_count, session_start_time, page_time

    while tracking and RUNNING:
        print("⏳ Preparing to send data...")
        time.sleep(30)

        print(f"🔍 Debug: USER_ID = {USER_ID}")
        print(f"⌨️ Keystrokes: {keystrokes} | 🖱️ Mouse Clicks: {mouse_clicks} | 🕒 Idle Count: {idle_count}")

        if USER_ID and session_start_time:
            total_session_seconds = time.time() - session_start_time

            total_page_time = sum(page_time.values())
            page_percentage = {
                k: (round((v / total_page_time) * 100, 2) if total_page_time > 0 else 0)
                for k, v in page_time.items()
            }

            data = {
                "user_id": USER_ID,
                "timestamp": get_utc_timestamp(),          # always keep UTC for consistency
                "pacific_date": get_pacific_date_string(), # 🆕 add this
                "mouse_distance": round(mouse_distance / 10_000, 2),
                "keystrokes": keystrokes,
                "mouse_clicks": mouse_clicks,
                "idle_count": idle_count,
                "total_session_time": round(total_session_seconds, 2),
                "page_time": page_time.copy(),
                "page_percentage": page_percentage
            }

            print(f"🚀 Sending Data to API: {data}")

            try:
                response = requests.post(TRACK_API, json=data, timeout=10)
                ok = 200 <= response.status_code < 300
                print(f"✅ Response: {response.status_code} - {response.text}")
            except Exception as e:
                ok = False
                print(f"❌ Error sending data: {e}")

            if ok:
                # Reset only on success
                mouse_distance = 0
                keystrokes = 0
                mouse_clicks = 0
                idle_count = 0
                for k in list(page_time.keys()):
                    page_time[k] = 0
            else:
                # Keep accumulating; cap to avoid runaway
                keystrokes = min(keystrokes, 100000)
                mouse_clicks = min(mouse_clicks, 50000)
                idle_count   = min(idle_count, 86400)


# ✅ Start/Stop Tracking (Fixed)
def start_tracking():
    global tracking, session_start_time, USER_ID, keep_awake_thread
    if not RUNNING:
        print("⚠️ Not starting tracking because RUNNING is False")
        return
    if USER_ID is None:
        print("❌ Cannot start tracking — USER_ID is None")
        return
    if tracking:
        return

    tracking = True

    # 🕒 Robust session start with carry-over cutoff (8h)
    now = time.time()
    if session_start_time is not None and (now - session_start_time) < 8 * 3600:
        print("🔄 Carrying over existing session (gap < 8h)")
    else:
        session_start_time = now
        print("🆕 Starting new session (gap >= 8h or first launch)")


    print("✅ Starting key & click tracking...")
    threading.Thread(target=track_mouse, daemon=True).start()
    threading.Thread(target=check_idle, daemon=True).start()
    threading.Thread(target=track_active_page, daemon=True).start()
    threading.Thread(target=track_keystrokes, daemon=True).start()
    threading.Thread(target=track_mouse_clicks, daemon=True).start()

    print("✅ Starting data sending thread...")
    threading.Thread(target=send_data, daemon=True).start()

    # 🌙 NEW: prevent system sleep while tracking
    keep_awake_thread = threading.Thread(target=_stay_awake_loop, daemon=True)
    keep_awake_thread.start()

def stop_tracking():
    global tracking, KEYBOARD_HOOK, MOUSE_CLICK_HANDLER, keep_awake_thread
    tracking = False
    # Clear the keep-awake request right away
    _set_keep_awake(False)

    try:
        if KEYBOARD_HOOK:
            keyboard.unhook(KEYBOARD_HOOK)
            KEYBOARD_HOOK = None
    except Exception:
        pass
    try:
        if MOUSE_CLICK_HANDLER:
            mouse.unhook(MOUSE_CLICK_HANDLER)
            MOUSE_CLICK_HANDLER = None
    except Exception:
        pass

    # Optional: let the keep-awake thread wind down
    try:
        if keep_awake_thread and keep_awake_thread.is_alive():
            keep_awake_thread.join(timeout=1)
    except Exception:
        pass
    keep_awake_thread = None


# ✅ New Login with stored creds 7-6-25
import json


def store_credentials(email, password):
    os.makedirs(os.path.dirname(CREDENTIALS_PATH), exist_ok=True)
    with open(CREDENTIALS_PATH, "w") as f:
        json.dump({"email": email, "password": password}, f)

def load_credentials():
    if os.path.exists(CREDENTIALS_PATH):
        try:
            with open(CREDENTIALS_PATH, "r") as f:
                return json.load(f)
        except:
            pass
    return {"email": "", "password": ""}

def create_login_window():
    global USER_ID

    login_window = tk.Tk()
    login_window.title("Reflexx Login")
    login_window.geometry("700x500")
    login_window.configure(bg="black")

    # ✅ Set Reflexx icon (works from source & PyInstaller)
    try:
        ico_path = resource_path("Reflexx_Icon.ico")
        if os.path.exists(ico_path):
            # Use native ICO for the window/titlebar icon
            login_window.iconbitmap(ico_path)
            # Also set a Tk PhotoImage (some setups use this for taskbar)
            img = Image.open(ico_path).resize((32, 32), Image.LANCZOS)
            icon = ImageTk.PhotoImage(img)
            login_window.iconphoto(True, icon)
            login_window._icon_ref = icon  # prevent GC
            print("✅ Using Reflexx_Icon.ico")
        else:
            print("⚠️ Reflexx_Icon.ico not found")
    except Exception as e:
        print(f"⚠️ Icon load failed: {e}")


    saved = load_credentials()

    tk.Label(login_window, text="Enter Email:", font=("Segoe UI", 10, "bold"), bg="black", fg="white").pack(pady=(20, 5))
    email_entry = tk.Entry(login_window, width=35, font=("Segoe UI", 10, "bold"))
    email_entry.insert(0, saved["email"])
    email_entry.pack(pady=5)

    tk.Label(login_window, text="Enter Password:", font=("Segoe UI", 10, "bold"), bg="black", fg="white").pack(pady=5)
    password_entry = tk.Entry(login_window, width=35, show="*", font=("Segoe UI", 10, "bold"))
    password_entry.insert(0, saved["password"])
    password_entry.pack(pady=5)

    def on_start():
        global USER_ID

        email = email_entry.get().strip()
        password = password_entry.get().strip()
        if not email or not password:
            print("⚠️ Email and password required.")
            return

        response = requests.post(AUTH_API, json={"email": email, "password": password}, timeout=10)
        print(f"🛠 Debug: Login Response - {response.status_code}, {response.text}")

        if response.status_code == 200:
            USER_ID = response.json().get("user_id")
            print(f"🔹 USER_ID Assigned: {USER_ID}")

            if USER_ID:
                fetch_and_cache_agency_hours(USER_ID)

                # ✅ Check work hours after fetching agency hours
                if not within_work_hours():
                    messagebox.showinfo("Outside Work Hours", "Reflexx cannot be started outside of work hours.")
                    return

                # ✅ Prevent duplicate launch AFTER work hour check (robust)
                if not acquire_lock_or_exit():
                    messagebox.showinfo("Reflexx Already Running", "Reflexx is already running in the background.")
                    return

                # ✅ Keep window open after Start (replace the four lines you showed with this)
                ensure_background_threads()
                start_tracking()

                # lock the login fields so they can't be changed mid-session
                email_entry.config(state="disabled")
                password_entry.config(state="disabled")

                # turn Start into a disabled "Running" indicator
                start_btn.config(text="Running...", state="disabled", bg="#0a7a30")

                
                # 🔎 quick health log
                print(f"[health] tracking={tracking}, RUNNING={RUNNING}")
        else:
            print("❌ Login failed.")

    def on_store_credentials():
        email = email_entry.get().strip()
        password = password_entry.get().strip()

        if not email or not password:
            messagebox.showinfo("Reflexx", "Please enter both Email and Password first.")
            return

        # save to C:\Users\<you>\AppData\Roaming\Reflexx\credentials.json
        store_credentials(email, password)

        # ✅ show the confirmation popup
        messagebox.showinfo("Credentials Saved", "Your credentials were saved securely.")

        # (nice touch) green status line if we have one
        try:
            status_lbl.config(text="Credentials saved ✔", fg="#22c55e")
        except Exception:
            pass

        # (optional) flash the button green for 0.6s
        try:
            orig = store_btn.cget("bg")
            store_btn.config(bg="#16a34a")
            store_btn.after(600, lambda: store_btn.config(bg=orig))
        except Exception:
            pass

    # 🟢 Everything BELOW this line is now OUTSIDE the function
    # ========== BUTTON STRIP (Start -> Stop -> Input Sales -> Store) ==========

    btn_frame = tk.Frame(login_window, bg="black")
    btn_frame.pack(pady=30)

    # ---- Handlers (define BEFORE creating buttons) ----
    def input_sales_now():
        if USER_ID:
            open_sales_calendar(USER_ID)
        else:
            messagebox.showinfo("Not Logged In", "Please click Login/Start first.")

    def on_stop():
        # 1) stop hooks/threads
        stop_tracking()
        # 2) signal shutdown
        global RUNNING
        RUNNING = False
        # 3) remove instance lock
        try: remove_lock()
        except Exception: pass
        # 4) close Tk
        try: login_window.destroy()
        except Exception: pass
        try:
            if tk._default_root: tk._default_root.quit()
        except Exception: pass
        # 5) exit process
        import sys, os
        try: sys.exit(0)
        except SystemExit: os._exit(0)

    # ---- Colors ----
    start_color = "#0a7a30"; hover_start = "#11a744"
    stop_color  = "#8b0000"; hover_stop  = "#b00000"
    sales_color = "#0071C1"; hover_sales = "#7a7a7a"
    store_color = "#5a5a5a"; hover_store = "#1ebeff"

    # ---- Buttons ----
    start_btn = tk.Button(
        btn_frame, text="Login/Start", command=on_start,
        width=16, bg=start_color, fg="white", font=("Segoe UI", 10, "bold"),
        disabledforeground="black"
    )
    stop_btn = tk.Button(
        btn_frame, text="Stop/Shutdown", command=on_stop,
        width=16, bg=stop_color, fg="white", font=("Segoe UI", 10, "bold")
    )
    sales_btn = tk.Button(
        btn_frame, text="Input Sales", command=input_sales_now,
        width=16, bg=sales_color, fg="white", font=("Segoe UI", 10, "bold")
    )
    store_btn = tk.Button(
        btn_frame, text="Store Credentials", command=on_store_credentials,
        width=16, bg=store_color, fg="white", font=("Segoe UI", 10, "bold")
    )

    # ---- Pack in the desired order (this sets the vertical order) ----
    start_btn.pack(pady=(0, 12))
    stop_btn.pack(pady=(0, 12))
    sales_btn.pack(pady=(0, 12))
    store_btn.pack(pady=(0, 0))

    # ---- Hover effects ----
    start_btn.bind("<Enter>", lambda e: start_btn.config(bg=hover_start))
    start_btn.bind("<Leave>", lambda e: start_btn.config(bg=start_color))
    stop_btn.bind("<Enter>",  lambda e: stop_btn.config(bg=hover_stop))
    stop_btn.bind("<Leave>",  lambda e: stop_btn.config(bg=stop_color))
    sales_btn.bind("<Enter>", lambda e: sales_btn.config(bg=hover_sales))
    sales_btn.bind("<Leave>", lambda e: sales_btn.config(bg=sales_color))
    store_btn.bind("<Enter>", lambda e: store_btn.config(bg=hover_store))
    store_btn.bind("<Leave>", lambda e: store_btn.config(bg=store_color))
    # =================================================================


    # ✅ Exit cleanly if window closed manually
    def on_closing():
        print("🚪 Login window closed. Exiting Reflexx.")
        login_window.destroy()
        sys.exit(0)

    login_window.protocol("WM_DELETE_WINDOW", on_closing)
    login_window.mainloop()

import atexit

# ⏯️ Launch the login + tracker flow
create_login_window()

# ✅ Only keep app running if user logged in
if USER_ID:
    while RUNNING:
        time.sleep(1)
    print("✅ Reflexx has shut down cleanly.")
else:
    print("⛔ Login failed or canceled. Exiting Reflexx.")
    sys.exit(0)

print("✅ Reflexx has shut down cleanly.")