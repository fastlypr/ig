"""
ig_dm.py — Safe Instagram DM sender with warmup system.

Setup:
  1. Share your Google Sheet publicly (Anyone with link → Viewer)
  2. Set SHEET_URL below to your Google Sheet URL
  3. Sheet must have columns: username, name, category (any variables)
  4. Edit templates.json with your message templates
  5. Run: python ig_dm.py
"""

import os
import re
import csv
import json
import time
import random
import requests
import io
import argparse
from datetime import date, datetime, timedelta
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # fallback
from instagrapi.exceptions import (
    ClientThrottledError, RateLimitError,
    UserNotFound, DirectError, LoginRequired,
    PleaseWaitFewMinutes
)
from ig_auth import get_client, _load_accounts_file

# ── Config ────────────────────────────────────────────────────────────────────

SHEET_URL      = "YOUR_GOOGLE_SHEET_URL_HERE"
LEADS_FOLDER   = "leads"            # local CSV files here override SHEET_URL
TEMPLATES_FILE = "templates.json"
DM_LOG_FILE    = "dm_log.csv"
SESSIONS_DIR   = "sessions"

# Warmup settings
WARMUP_START      = 3    # DMs on day 1
WARMUP_MAX        = 30   # hard cap per day
WARMUP_GROWTH_MIN = 1    # min increase per day
WARMUP_GROWTH_MAX = 2    # max increase per day

# Delay between DMs — scales with warmup day. See get_delay_range() below.
# Floor is 3-6 min (180s–360s); early warmup days use 5-8 min to stay extra safe.
DELAY_JITTER = 15    # ±15 sec jitter on every delay

# Rate limit pause
RATE_LIMIT_PAUSE_MIN = 900    # 15 min
RATE_LIMIT_PAUSE_MAX = 1800   # 30 min

# Active hours (24h format) — DMs only sent inside this window
# Interpreted in the proxy's local timezone, not the server's.
ACTIVE_START_HOUR = 9    # 9 AM (weekday); weekend shifted +1
ACTIVE_END_HOUR   = 22   # 10 PM (weekday); weekend shifted +1
DEAD_ZONE         = (0, 7)   # hours local — never send

# Weighted time windows (start_hour, end_hour, weight). Weights must sum ≈ 1.
WEEKDAY_WINDOWS = [
    (9,  11, 0.20),   # morning coffee scroll
    (12, 14, 0.15),   # lunch break
    (14, 18, 0.15),   # afternoon filler
    (18, 22, 0.50),   # evening peak
]
WEEKEND_WINDOWS = [
    (10, 12, 0.15),   # slow morning
    (12, 15, 0.15),   # midday
    (15, 19, 0.20),   # afternoon
    (19, 23, 0.50),   # big evening
]

DEFAULT_TZ = "Asia/Kolkata"   # fallback when proxy country can't be resolved

# Batch scheduling (spreads daily DMs across the day)
BATCH_SIZE_MIN = 2     # min DMs per batch
BATCH_SIZE_MAX = 6     # max DMs per batch


# ── Proxy → Timezone ──────────────────────────────────────────────────────────

COUNTRY_TZ = {
    "us": "America/New_York",    "ua": "Europe/Kyiv",
    "gb": "Europe/London",       "uk": "Europe/London",
    "de": "Europe/Berlin",       "fr": "Europe/Paris",
    "in": "Asia/Kolkata",        "br": "America/Sao_Paulo",
    "ca": "America/Toronto",     "au": "Australia/Sydney",
    "jp": "Asia/Tokyo",          "ae": "Asia/Dubai",
    "sg": "Asia/Singapore",      "es": "Europe/Madrid",
    "it": "Europe/Rome",         "nl": "Europe/Amsterdam",
    "pl": "Europe/Warsaw",       "se": "Europe/Stockholm",
    "mx": "America/Mexico_City", "id": "Asia/Jakarta",
    "tr": "Europe/Istanbul",     "ru": "Europe/Moscow",
    "kr": "Asia/Seoul",          "th": "Asia/Bangkok",
    "vn": "Asia/Ho_Chi_Minh",    "ph": "Asia/Manila",
    "za": "Africa/Johannesburg", "ar": "America/Argentina/Buenos_Aires",
    "cl": "America/Santiago",    "co": "America/Bogota",
    "pe": "America/Lima",        "pk": "Asia/Karachi",
    "bd": "Asia/Dhaka",          "eg": "Africa/Cairo",
    "ng": "Africa/Lagos",        "ke": "Africa/Nairobi",
    "il": "Asia/Jerusalem",      "sa": "Asia/Riyadh",
    "my": "Asia/Kuala_Lumpur",   "ie": "Europe/Dublin",
    "ch": "Europe/Zurich",       "at": "Europe/Vienna",
    "be": "Europe/Brussels",     "pt": "Europe/Lisbon",
    "gr": "Europe/Athens",       "no": "Europe/Oslo",
    "dk": "Europe/Copenhagen",   "fi": "Europe/Helsinki",
    "ro": "Europe/Bucharest",    "cz": "Europe/Prague",
    "hu": "Europe/Budapest",     "bg": "Europe/Sofia",
    "hk": "Asia/Hong_Kong",      "tw": "Asia/Taipei",
    "nz": "Pacific/Auckland",
}

US_STATE_TZ = {
    "california":"America/Los_Angeles", "ca":"America/Los_Angeles",
    "oregon":"America/Los_Angeles",     "washington":"America/Los_Angeles",
    "nevada":"America/Los_Angeles",
    "arizona":"America/Phoenix",
    "colorado":"America/Denver", "utah":"America/Denver",
    "newmexico":"America/Denver", "montana":"America/Denver",
    "wyoming":"America/Denver",  "idaho":"America/Denver",
    "texas":"America/Chicago",        "illinois":"America/Chicago",
    "minnesota":"America/Chicago",    "missouri":"America/Chicago",
    "louisiana":"America/Chicago",    "oklahoma":"America/Chicago",
    "arkansas":"America/Chicago",     "kansas":"America/Chicago",
    "iowa":"America/Chicago",         "wisconsin":"America/Chicago",
    "alabama":"America/Chicago",      "mississippi":"America/Chicago",
    "tennessee":"America/Chicago",    "nebraska":"America/Chicago",
    "newyork":"America/New_York",  "ny":"America/New_York",
    "florida":"America/New_York",  "fl":"America/New_York",
    "georgia":"America/New_York",  "pennsylvania":"America/New_York",
    "ohio":"America/New_York",     "michigan":"America/New_York",
    "virginia":"America/New_York", "northcarolina":"America/New_York",
    "newjersey":"America/New_York","massachusetts":"America/New_York",
    "maryland":"America/New_York", "indiana":"America/New_York",
    "alaska":"America/Anchorage",
    "hawaii":"Pacific/Honolulu",
}


def parse_proxy_country(proxy):
    """Extract (country_code, state) from a proxy URL. Returns (None, None) if not found."""
    if not proxy:
        return None, None
    p = proxy.lower()
    country = None
    state   = None
    # Decodo/Smartproxy style: country-us, country.us
    m = re.search(r'country[-.]([a-z]{2})\b', p)
    if m: country = m.group(1)
    # DataImpulse style: cr.us, __cr.us
    if not country:
        m = re.search(r'(?:^|[^a-z])cr\.([a-z]{2})\b', p)
        if m: country = m.group(1)
    # Generic _cc-XX / _cc.XX
    if not country:
        m = re.search(r'[_-]cc[-.]([a-z]{2})\b', p)
        if m: country = m.group(1)
    # region-XX / zone-XX (less common)
    if not country:
        m = re.search(r'(?:region|zone)[-.]([a-z]{2})\b', p)
        if m: country = m.group(1)

    # State / region
    m = re.search(r'state[-.]([a-z_]+)', p)
    if m: state = m.group(1)
    if not state:
        m = re.search(r'region[-.]([a-z_]+)', p)
        # avoid re-matching the country code with "region.us"
        if m and len(m.group(1)) > 2:
            state = m.group(1)
    return country, state


def resolve_proxy_tz(proxy, default=DEFAULT_TZ):
    """
    Return (ZoneInfo, human_label) for a proxy URL.
    Falls back to DEFAULT_TZ if country can't be detected.
    """
    country, state = parse_proxy_country(proxy)
    if country == "us" and state:
        key = state.replace("_", "").replace("-", "")
        tz = US_STATE_TZ.get(key)
        if tz:
            return ZoneInfo(tz), f"US/{state} ({tz})"
    if country:
        tz = COUNTRY_TZ.get(country)
        if tz:
            return ZoneInfo(tz), f"{country.upper()} ({tz})"
    try:
        return ZoneInfo(default), f"default ({default})"
    except Exception:
        return ZoneInfo("UTC"), "UTC"


# ── Google Sheets (no API, direct CSV export) ─────────────────────────────────

def _sheet_to_csv_url(url):
    """Convert any Google Sheets URL to a direct CSV export URL."""
    # Extract the sheet ID
    if "/d/" in url:
        sheet_id = url.split("/d/")[1].split("/")[0]
    else:
        raise ValueError("Invalid Google Sheets URL. Must contain /d/<sheet_id>/")
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"


def load_sheet(url):
    """Fetch Google Sheet rows directly as CSV — no API needed, just public share link."""
    if url == "YOUR_GOOGLE_SHEET_URL_HERE":
        raise ValueError(
            "Set SHEET_URL in ig_dm.py to your Google Sheet URL.\n"
            "Make sure the sheet is shared: Anyone with link → Viewer."
        )
    csv_url = _sheet_to_csv_url(url)
    response = requests.get(csv_url, timeout=15)
    if response.status_code != 200:
        raise ConnectionError(
            f"Could not fetch sheet (HTTP {response.status_code}).\n"
            "Make sure the sheet is shared as 'Anyone with link can view'."
        )
    reader = csv.DictReader(io.StringIO(response.text))
    rows = []
    for i, row in enumerate(reader, start=2):
        rows.append({"_row": i, **normalize_row(row)})
    return rows


def normalize_row(row):
    """
    Lowercase column names, replace spaces with underscores.
    Adds a `name` alias for first-name columns (for template personalization).
    """
    normalized = {}
    for k, v in row.items():
        if not k:
            continue
        key = k.strip().lower().replace(" ", "_")
        val = v.strip() if isinstance(v, str) else v
        normalized[key] = val

    # Create {name} alias from any common first-name column
    if not normalized.get("name"):
        for possible in ["featured_first_name", "first_name", "firstname", "fname"]:
            if normalized.get(possible):
                normalized["name"] = normalized[possible]
                break

    return normalized


def load_local_leads():
    """Load leads from the most recent CSV in LEADS_FOLDER, or None if empty."""
    if not os.path.isdir(LEADS_FOLDER):
        return None
    csv_files = [f for f in os.listdir(LEADS_FOLDER) if f.lower().endswith(".csv")]
    if not csv_files:
        return None
    # Pick the most recently modified file
    csv_files.sort(key=lambda f: os.path.getmtime(os.path.join(LEADS_FOLDER, f)), reverse=True)
    path = os.path.join(LEADS_FOLDER, csv_files[0])
    print(f"[+] Loading leads from local file: {path}")
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = []
        for i, row in enumerate(reader, start=2):
            rows.append({"_row": i, **normalize_row(row)})
    return rows


def load_leads():
    """Prefer local CSV in LEADS_FOLDER; fall back to Google Sheet URL."""
    local = load_local_leads()
    if local is not None:
        return local
    return load_sheet(SHEET_URL)


# ── Already-sent checker (from dm_log.csv) ───────────────────────────────────

def load_sent_usernames():
    """Return a set of usernames already successfully sent from dm_log.csv."""
    sent = set()
    if not os.path.exists(DM_LOG_FILE):
        return sent
    with open(DM_LOG_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("status", "").strip().lower() == "sent":
                sent.add(row.get("username", "").strip().lower())
    return sent


# ── Warmup Engine ─────────────────────────────────────────────────────────────

def _warmup_file(account):
    return f"{SESSIONS_DIR}/{account}_warmup.json"


def load_warmup(account):
    path = _warmup_file(account)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {
        "day":           1,
        "daily_sent":    0,
        "daily_limit":   WARMUP_START,
        "daily_target":  WARMUP_START,   # rolled per-day with variance
        "target_date":   str(date.today()),
        "last_run_date": str(date.today()),
        "total_sent":    0,
    }


def save_warmup(account, state):
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    with open(_warmup_file(account), "w") as f:
        json.dump(state, f, indent=2)


def _roll_daily_target(daily_limit):
    """Pick today's actual send target with human-like variance.

    Real users don't hit their cap every day. Distribution:
      ~7%  rest day (0 DMs — sick / busy / traveling)
      ~10% light day (40-60% of cap — distracted)
      ~25% chill day (70-90% of cap)
      ~58% full day (100% of cap)
    """
    roll = random.random()
    if roll < 0.07:
        return 0  # rest day
    if roll < 0.17:
        lo = max(1, int(daily_limit * 0.4))
        hi = max(lo, int(daily_limit * 0.6))
        return random.randint(lo, hi)
    if roll < 0.42:
        lo = max(1, int(daily_limit * 0.7))
        hi = max(lo, int(daily_limit * 0.9))
        return random.randint(lo, hi)
    return daily_limit  # full-effort day


def update_warmup_day(account, state):
    """If it's a new calendar day, reset daily counter and increase limit."""
    today = str(date.today())
    if state["last_run_date"] != today:
        growth    = random.randint(WARMUP_GROWTH_MIN, WARMUP_GROWTH_MAX)
        new_limit = min(state["daily_limit"] + growth, WARMUP_MAX)
        new_target = _roll_daily_target(new_limit)
        print(f"  [warmup] New day for @{account}! Limit: {state['daily_limit']} → {new_limit} DMs/day"
              f"  |  Today's target: {new_target}"
              + ("  💤 REST DAY" if new_target == 0 else
                 "  💪 full day" if new_target == new_limit else
                 f"  📉 light day ({int(new_target/new_limit*100)}%)"))
        state["day"]           += 1
        state["daily_sent"]    = 0
        state["daily_limit"]   = new_limit
        state["daily_target"]  = new_target
        state["target_date"]   = today
        state["last_run_date"] = today
        save_warmup(account, state)
    elif state.get("target_date") != today:
        # Existing warmup file from before this feature — backfill today's target
        state["daily_target"] = _roll_daily_target(state["daily_limit"])
        state["target_date"]  = today
        save_warmup(account, state)
    return state


def effective_limit(state):
    """Today's actual ceiling = min(daily_limit, daily_target).
    Falls back to daily_limit for warmup files written before this feature."""
    target = state.get("daily_target", state["daily_limit"])
    return min(state["daily_limit"], target)


def can_send(account, state):
    return state["daily_sent"] < effective_limit(state)


def record_send(account, state):
    state["daily_sent"] += 1
    state["total_sent"] += 1
    save_warmup(account, state)
    return state


# ── Template Engine ───────────────────────────────────────────────────────────

def load_templates():
    if not os.path.exists(TEMPLATES_FILE):
        raise FileNotFoundError(f"'{TEMPLATES_FILE}' not found.")
    with open(TEMPLATES_FILE) as f:
        templates = json.load(f)
    if not templates:
        raise ValueError("templates.json is empty.")
    return templates


def resolve_spintax(template):
    """
    Resolve {option1|option2|option3} spintax — picks one at random.
    Ignores plain {variable} placeholders (no pipe = sheet variable, left alone).
    Supports nested spintax: {Hello|{Hi|Hey} there}
    """
    import re
    # Keep resolving until no spintax remains (handles nesting)
    while True:
        # Match innermost {a|b|c} — no nested braces inside
        match = re.search(r'\{([^{}]*\|[^{}]*)\}', template)
        if not match:
            break
        options = match.group(1).split("|")
        chosen  = random.choice(options).strip()
        template = template[:match.start()] + chosen + template[match.end():]
    return template


def fill_template(template, variables: dict):
    """Resolve spintax first, then fill {variable} placeholders from sheet row."""
    import re
    template = resolve_spintax(template)
    class SafeDict(dict):
        def __missing__(self, key):
            return ""
    try:
        result = template.format_map(SafeDict({k.lower(): v for k, v in variables.items()}))
    except Exception:
        result = template
    # Clean up double spaces + spaces before punctuation (happens if a var is empty)
    result = re.sub(r"\s+", " ", result)
    result = re.sub(r"\s+([!?.,;:])", r"\1", result)
    return result.strip()


# ── CSV Logger ────────────────────────────────────────────────────────────────

LOG_HEADERS = ["timestamp", "account", "username", "user_id", "message_sent", "status", "error"]


def init_log():
    if not os.path.exists(DM_LOG_FILE):
        with open(DM_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=LOG_HEADERS).writeheader()


def log_dm(account, username, user_id, message, status, error=""):
    with open(DM_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=LOG_HEADERS).writerow({
            "timestamp":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "account":      account,
            "username":     username,
            "user_id":      str(user_id),
            "message_sent": message,
            "status":       status,
            "error":        error,
        })


# ── DM Sender ─────────────────────────────────────────────────────────────────

def send_dm(cl, username, message):
    """Returns (user_id, success, error). success=None means rate limited."""
    try:
        user_id = cl.user_id_from_username(username)
    except UserNotFound:
        return None, False, "UserNotFound"
    except Exception as e:
        return None, False, str(e)

    try:
        cl.direct_send(message, user_ids=[user_id])
        return user_id, True, ""
    except (ClientThrottledError, RateLimitError, PleaseWaitFewMinutes) as e:
        return user_id, None, f"RateLimit:{type(e).__name__}"
    except Exception as e:
        return user_id, False, str(e)


def get_delay_range(warmup_day):
    """DM-to-DM delay (seconds) scaled by warmup day.
    Slower early, then steady floor of 3-6 min once account is trusted."""
    if warmup_day <= 3:
        return (300, 480)   # 5-8 min — fresh account, maximum caution
    if warmup_day <= 7:
        return (240, 420)   # 4-7 min — early trust building
    return (180, 360)       # 3-6 min — floor, kept forever


def get_batch_gap_range(warmup_day):
    """Gap between batches (seconds) scaled by warmup day."""
    if warmup_day <= 3:
        return (2 * 3600, 4 * 3600)   # 2-4 h
    if warmup_day <= 7:
        return (90 * 60, 2 * 3600)    # 1.5-2 h
    if warmup_day <= 14:
        return (60 * 60, 90 * 60)     # 1-1.5 h
    return (45 * 60, 90 * 60)         # 45-90 min


def human_delay(warmup_day=1):
    dmin, dmax = get_delay_range(warmup_day)
    base  = random.randint(dmin, dmax)
    total = max(60, base + random.randint(-DELAY_JITTER, DELAY_JITTER))
    print(f"  [delay] Waiting {total // 60}m {total % 60}s before next DM (Day {warmup_day})...")
    time.sleep(total)


def rate_limit_pause():
    pause = random.randint(RATE_LIMIT_PAUSE_MIN, RATE_LIMIT_PAUSE_MAX)
    print(f"\n  [!] Rate limit hit. Pausing {pause // 60} minutes...")
    time.sleep(pause)


# ── Batch Scheduler ───────────────────────────────────────────────────────────

def _windows_for_date(d):
    """Return the weighted time windows for a given date (weekday vs weekend)."""
    return WEEKEND_WINDOWS if d.weekday() >= 5 else WEEKDAY_WINDOWS


def _pick_batch_size(warmup_day, remaining):
    """Pick a single batch size from a human-like weighted distribution.
    Range 2-6 DMs (rare 7 at Day 15+). Capped by remaining DMs.

    Weights bias toward asymmetric splits (avoid all-equal patterns).
    """
    if warmup_day <= 3:
        weights = [(2, 60), (3, 40)]                              # 2-3, mostly 2
    elif warmup_day <= 7:
        weights = [(3, 30), (4, 40), (5, 25), (6, 5)]             # 3-6, asymmetric center on 4
    elif warmup_day <= 14:
        weights = [(3, 20), (4, 30), (5, 30), (6, 20)]            # 3-6, even spread
    else:
        weights = [(3, 10), (4, 25), (5, 30), (6, 30), (7, 5)]    # 4-6 mostly, rare 7

    sizes = [s for s, _ in weights]
    probs = [w for _, w in weights]
    pick  = random.choices(sizes, weights=probs)[0]
    return min(pick, remaining)


def _split_into_batches(total, warmup_day):
    """Greedy split of total DMs into a list of human-like batch sizes (2-6).

    - Avoids stranded 1-DM batches (absorbs into the last batch instead).
    - Anti-repeat: if all batches end up identical (e.g. [4,4,4]), re-rolls
      once to force variation when math allows.
    - Order randomized so largest batches don't always come first.
    """
    if total <= 0:
        return []
    if total < 2:
        return [total]

    def _one_split():
        s = []
        remaining = total
        while remaining > 0:
            pick = _pick_batch_size(warmup_day, remaining)
            # Avoid leaving a stranded 1
            if 0 < remaining - pick < 2:
                pick = remaining
            s.append(pick)
            remaining -= pick
        return s

    sizes = _one_split()

    # Anti-repeat: if 2+ batches all identical, retry once
    if len(sizes) >= 2 and len(set(sizes)) == 1:
        retry = _one_split()
        if len(set(retry)) > 1:
            sizes = retry

    random.shuffle(sizes)
    return sizes


def plan_batches(total_limit, start_from=None, warmup_day=1, proxy_tz=None):
    """
    Split today's DM budget into randomized batches, weighted by real human
    activity patterns, in the proxy's local timezone.

    Weights (weekday):
      20% morning (9-11), 15% midday (12-14),
      15% afternoon (14-18), 50% evening (18-22)
    Dead zone (00:00-07:00 local) → pushed to next morning.

    Batch count scales with warmup day (Day 1-3: 1-2, ..., Day 15+: 4-6).
    Returns list of (start_datetime, batch_size) — datetimes are NAIVE in
    the server's local time (compatible with datetime.now() elsewhere).
    """
    if total_limit <= 0:
        return []

    # Batch sizes drawn from a weighted distribution per warmup day —
    # mimics how real users send DMs (sometimes 1, sometimes 5, never identical).
    # Mean ~3, range 1-6. Smaller days early, fuller distribution as account ages.
    sizes = _split_into_batches(total_limit, warmup_day)

    # Timezone setup
    if proxy_tz is None:
        proxy_tz = ZoneInfo(DEFAULT_TZ)

    now_local = datetime.now(proxy_tz) if start_from is None else start_from.astimezone(proxy_tz)

    # Pacing parameters
    gap_min, gap_max     = get_batch_gap_range(warmup_day)
    delay_min, delay_max = get_delay_range(warmup_day)

    # Earliest slot: now + small warm-up delay, bumped out of dead zone / past-evening
    earliest = now_local + timedelta(minutes=random.randint(3, 15))
    if earliest.hour < DEAD_ZONE[1] or earliest.hour >= 22:
        # Advance to next morning's first window start, with ±30 min jitter
        next_day = earliest.date()
        if earliest.hour >= 22:
            next_day = next_day + timedelta(days=1)
        first_ws = _windows_for_date(next_day)[0][0]
        earliest = datetime(
            next_day.year, next_day.month, next_day.day,
            first_ws, random.randint(0, 30),
            tzinfo=proxy_tz,
        )

    planned = []
    i = 0
    max_iterations = len(sizes) * 5  # safety against loops
    while i < len(sizes) and max_iterations > 0:
        max_iterations -= 1
        size = sizes[i]
        windows = _windows_for_date(earliest.date())

        # Find windows on earliest.date() that still have room
        day = earliest.date()
        viable = []
        viable_weights = []
        for (ws, we, w) in windows:
            win_end = datetime(day.year, day.month, day.day, we, 0, tzinfo=proxy_tz)
            if win_end - earliest > timedelta(minutes=5):
                viable.append((ws, we))
                viable_weights.append(w)

        if not viable:
            # No room left today — jump to tomorrow's first window with jitter
            tomorrow = day + timedelta(days=1)
            first_ws = _windows_for_date(tomorrow)[0][0]
            earliest = datetime(
                tomorrow.year, tomorrow.month, tomorrow.day,
                first_ws, random.randint(0, 30),
                tzinfo=proxy_tz,
            )
            continue  # retry same batch

        ws, we = random.choices(viable, weights=viable_weights)[0]
        win_start = datetime(day.year, day.month, day.day, ws, 0, tzinfo=proxy_tz)
        win_end   = datetime(day.year, day.month, day.day, we, 0, tzinfo=proxy_tz)
        slot_start = max(win_start, earliest)

        # Random placement inside the remaining window
        range_secs = int((win_end - slot_start).total_seconds())
        offset = random.randint(0, max(1, range_secs - 60))
        slot = slot_start + timedelta(seconds=offset)
        planned.append((slot, size))

        # Advance earliest: batch sending time + rest gap
        avg_delay      = (delay_min + delay_max) // 2
        batch_duration = size * avg_delay
        gap = random.randint(gap_min, gap_max)
        earliest = slot + timedelta(seconds=batch_duration + gap)
        i += 1

    planned.sort(key=lambda x: x[0])

    # Convert aware → naive system-local (keeps downstream wait_until() happy)
    return [(dt.astimezone().replace(tzinfo=None), sz) for (dt, sz) in planned]


BATCH_PLAN_FILE = os.path.join(SESSIONS_DIR, "batch_plan.json")


def save_batch_plan(batches):
    """Persist batch plan so we can resume after restart.
    batches = list of dicts {start: datetime, size: int, done: int}"""
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    data = {
        "saved_at": datetime.now().isoformat(),
        "batches": [
            {
                "start": b["start"].isoformat() if isinstance(b["start"], datetime) else b["start"],
                "size":  int(b["size"]),
                "done":  int(b.get("done", 0)),
            }
            for b in batches
        ],
    }
    with open(BATCH_PLAN_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_batch_plan():
    if not os.path.exists(BATCH_PLAN_FILE):
        return None
    try:
        with open(BATCH_PLAN_FILE) as f:
            data = json.load(f)
        return [
            {
                "start": datetime.fromisoformat(b["start"]),
                "size":  int(b["size"]),
                "done":  int(b.get("done", 0)),
            }
            for b in data.get("batches", [])
        ]
    except Exception as e:
        print(f"[!] Could not read batch plan ({e}) — starting fresh.")
        return None


def clear_batch_plan():
    if os.path.exists(BATCH_PLAN_FILE):
        try:
            os.remove(BATCH_PLAN_FILE)
        except OSError:
            pass


def reconcile_plan(batches, stale_hours=12):
    """
    Adjust a saved plan for current time.

    - Completed batches (done >= size) → dropped
    - Past-due batches with work remaining → merged into ONE catch-up batch
      scheduled 2-5 min from now
    - Future batches → kept at original time, size = remaining work
    - If the most recent batch is more than `stale_hours` old, the plan is
      considered abandoned (e.g. VPS was down overnight) and we plan fresh

    Returns new list of dicts, or None if plan is unusable.
    """
    if not batches:
        return None

    now    = datetime.now()
    latest = max(b["start"] for b in batches)
    if (now - latest).total_seconds() > stale_hours * 3600:
        print(f"[resume] Saved plan is older than {stale_hours}h — discarding.")
        return None

    catchup_size = 0
    future = []
    for b in batches:
        remaining = b["size"] - b["done"]
        if remaining <= 0:
            continue
        if b["start"] <= now:
            catchup_size += remaining
        else:
            future.append({"start": b["start"], "size": remaining, "done": 0})

    result = []
    if catchup_size > 0:
        catchup_time = now + timedelta(seconds=random.randint(120, 300))
        result.append({"start": catchup_time, "size": catchup_size, "done": 0})
        print(f"[resume] {catchup_size} DM(s) were past-due — catch-up batch at {catchup_time.strftime('%H:%M')}")
    if future:
        print(f"[resume] {len(future)} future batch(es) preserved from previous plan")
    result.extend(future)
    result.sort(key=lambda b: b["start"])
    return result or None


def wait_until(target_time, label=""):
    """Sleep until target_time, with periodic status updates."""
    while True:
        now = datetime.now()
        remaining = (target_time - now).total_seconds()
        if remaining <= 0:
            return
        # Show countdown in hours/minutes
        hrs  = int(remaining // 3600)
        mins = int((remaining % 3600) // 60)
        if hrs > 0:
            print(f"  [sleep] {label} in {hrs}h {mins}m (at {target_time.strftime('%H:%M')})")
            time.sleep(min(remaining, 1800))  # wake every 30 min to show heartbeat
        else:
            print(f"  [sleep] {label} in {mins}m (at {target_time.strftime('%H:%M')})")
            time.sleep(min(remaining, 60))


# ── Main ──────────────────────────────────────────────────────────────────────

def send_one_dm(pending, templates, accounts, warmup_states, clients, acc_index):
    """
    Send a single DM to the next pending recipient.
    Returns (new_acc_index, status) where status is "sent", "failed", or "no_pending" / "no_account".
    """
    if not pending:
        return acc_index, "no_pending"

    # Pick eligible account (round-robin)
    eligible = [a for a in accounts if can_send(a, warmup_states[a])]
    if not eligible:
        return acc_index, "no_account"

    account = eligible[acc_index % len(eligible)]
    acc_index += 1

    # Lazy-load client
    if account not in clients:
        print(f"  [*] Logging in @{account}...")
        try:
            clients[account] = get_client(account)
        except Exception as e:
            print(f"  [!] Login failed for @{account}: {e}")
            return acc_index, "failed"

    cl  = clients[account]
    row = pending.pop(0)
    username = str(row.get("username", "")).strip().lower()

    template = random.choice(templates)
    message  = fill_template(template, row)

    print(f"  @{account} → @{username}")
    print(f"    {message[:90]}{'...' if len(message) > 90 else ''}")

    user_id, success, error = send_dm(cl, username, message)
    if success is None:
        rate_limit_pause()
        user_id, success, error = send_dm(cl, username, message)

    if success:
        print(f"  [✓] Sent!")
        log_dm(account, username, user_id, message, "sent")
        warmup_states[account] = record_send(account, warmup_states[account])
        ws = warmup_states[account]
        print(f"  [warmup] @{account}: {ws['daily_sent']}/{ws['daily_limit']} today")
        return acc_index, "sent"
    else:
        print(f"  [!] Failed: {error}")
        log_dm(account, username, user_id or "", message, "failed", error)
        return acc_index, "failed"


def run_batch(batch_size, pending, templates, accounts, warmup_states, clients, acc_index):
    """Send a batch of DMs with warmup-day-scaled gaps between each."""
    sent_in_batch = 0
    failed_in_batch = 0

    for i in range(batch_size):
        if not pending:
            break

        # Capture the account that WILL send the next DM (before send_one_dm rotates)
        sending_acc = accounts[acc_index % len(accounts)]

        acc_index, status = send_one_dm(
            pending, templates, accounts, warmup_states, clients, acc_index
        )

        if status == "sent":
            sent_in_batch += 1
        elif status == "failed":
            failed_in_batch += 1
        elif status == "no_account":
            print("  [!] All accounts hit daily limit — ending batch early.")
            break
        elif status == "no_pending":
            break

        # Gap between DMs within a batch (skip after the last one)
        if i < batch_size - 1 and pending:
            # Use sending account's warmup day to set delay length
            day = warmup_states.get(sending_acc, {}).get("day", 1)
            human_delay(warmup_day=day)

    return acc_index, sent_in_batch, failed_in_batch


# ── Account Picker ────────────────────────────────────────────────────────────

def pick_accounts(all_accounts, cfg_data):
    """
    Interactive picker: show all configured accounts, let user pick which to
    use for this DM run.
    """
    print("\n── Select Account(s) For This Run ──")
    default      = cfg_data.get("default")
    accounts_cfg = cfg_data.get("accounts", {})
    for idx, acc in enumerate(all_accounts, 1):
        info  = accounts_cfg.get(acc, {})
        proxy = info.get("proxy") or "no proxy"
        tag   = " [default]" if acc == default else ""
        print(f"  {idx}. @{acc}{tag}  ({proxy})")

    print("\n  Options:")
    print("    • Enter a number (e.g. 1) for one account")
    print("    • Enter comma-separated numbers (e.g. 1,3) for multiple")
    print("    • Type 'all' to use every account")
    print("    • Press ENTER to use default only")

    choice = input("\nChoice: ").strip().lower()

    if not choice:
        if default and default in all_accounts:
            return [default]
        return [all_accounts[0]]

    if choice == "all":
        return all_accounts

    selected = []
    for part in choice.split(","):
        part = part.strip()
        if part.isdigit():
            idx = int(part) - 1
            if 0 <= idx < len(all_accounts):
                selected.append(all_accounts[idx])
            else:
                print(f"[!] Invalid number: {part}")
        else:
            print(f"[!] Skipping invalid input: {part}")

    # Dedupe while preserving order
    seen   = set()
    result = []
    for a in selected:
        if a not in seen:
            seen.add(a)
            result.append(a)
    return result


def main():
    # Parse CLI args
    parser = argparse.ArgumentParser(description="Instagram DM sender with warmup & batch scheduling")
    parser.add_argument("--auto",    action="store_true", help="Non-interactive mode (for systemd/background). Uses all configured accounts, follows planned schedule.")
    parser.add_argument("--account", type=str, default=None, help="Use only this specific account (comma-separated for multiple)")
    args = parser.parse_args()

    print("\n── ig_dm — Instagram DM Sender (All-Day Mode) ──\n")
    if args.auto:
        print("[*] Running in AUTO mode (non-interactive)\n")

    # Load templates (once — reused across days)
    templates = load_templates()
    print(f"[+] Loaded {len(templates)} message templates")

    init_log()

    # Load accounts
    cfg_data = _load_accounts_file()
    all_accounts = list(cfg_data.get("accounts", {}).keys())
    if not all_accounts:
        print("[!] No accounts configured. Run `python ig_auth.py` first.")
        return

    # ── Account selection ─────────────────────────────────────────────────
    if args.account:
        requested = [a.strip().lower() for a in args.account.split(",")]
        accounts  = [a for a in requested if a in all_accounts]
        missing   = [a for a in requested if a not in all_accounts]
        if missing:
            print(f"[!] Accounts not found: {', '.join('@'+a for a in missing)}")
    elif args.auto:
        # Non-interactive → use all accounts
        accounts = all_accounts
        print(f"[*] Auto mode: using all {len(accounts)} configured account(s)")
    else:
        accounts = pick_accounts(all_accounts, cfg_data)

    if not accounts:
        print("[!] No accounts selected. Exiting.")
        return

    clients       = {}
    acc_index     = 0
    grand_total   = 0
    grand_failed  = 0

    # Resolve the schedule's timezone from the primary account's proxy.
    # (All accounts in one run share a schedule; we use the first selected.)
    primary_proxy = cfg_data.get("accounts", {}).get(accounts[0], {}).get("proxy")
    proxy_tz, tz_label = resolve_proxy_tz(primary_proxy)
    now_local = datetime.now(proxy_tz)

    print(f"\n[+] Using {len(accounts)} account(s): {', '.join('@'+a for a in accounts)}")
    print(f"[+] Schedule clock: {tz_label}  —  currently {now_local.strftime('%a %H:%M')} local")
    is_weekend = now_local.weekday() >= 5
    active_windows = "weekend" if is_weekend else "weekday"
    print(f"[+] Mode: {active_windows} — weighted windows (morning/midday/afternoon/evening)")
    print(f"[+] Dead zone (never sends): {DEAD_ZONE[0]:02d}:00 – {DEAD_ZONE[1]:02d}:00 proxy-local")
    print(f"[+] Script will run all day and sleep between batches.\n")

    # ── Day loop ──────────────────────────────────────────────────────────
    while True:
        print("\n" + "═" * 60)
        print(f"  Day cycle started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("═" * 60 + "\n")

        # Reload recipients fresh each day (local CSV preferred, sheet fallback)
        print("[*] Loading recipients...")
        try:
            all_rows = load_leads()
        except Exception as e:
            print(f"[!] Failed to load leads: {e}")
            print("    Retrying in 10 min...")
            time.sleep(600)
            continue

        already_sent = load_sent_usernames()
        pending = []
        for row in all_rows:
            username = str(row.get("username", "")).strip().lower()
            if not username or username in already_sent:
                continue
            pending.append(row)

        print(f"[+] {len(all_rows)} rows in sheet | {len(already_sent)} already sent | {len(pending)} pending\n")

        if not pending:
            print("[✓] Nothing left to send. All recipients DMed. Exiting.")
            break

        # Refresh warmup state for each account (handles new day)
        warmup_states = {}
        for acc in accounts:
            warmup_states[acc] = load_warmup(acc)
            warmup_states[acc] = update_warmup_day(acc, warmup_states[acc])

        # Today's total capacity = sum of remaining EFFECTIVE limits across accounts
        # (effective = min(daily_limit, daily_target) — accounts for variance / rest days)
        total_remaining = sum(
            max(0, effective_limit(ws) - ws["daily_sent"])
            for ws in warmup_states.values()
        )
        total_remaining = min(total_remaining, len(pending))

        print("── Account Warmup Status ──")
        for acc in accounts:
            ws  = warmup_states[acc]
            tgt = effective_limit(ws)
            tag = ""
            if tgt == 0:
                tag = "  💤 REST DAY"
            elif tgt < ws["daily_limit"]:
                tag = f"  📉 chill day (target {tgt}/{ws['daily_limit']})"
            print(f"  @{acc} — Day {ws['day']} | {ws['daily_sent']}/{tgt} today "
                  f"(cap {ws['daily_limit']}) | Total: {ws['total_sent']}{tag}")
        print(f"\n[+] Total DMs to send today: {total_remaining}\n")

        if total_remaining == 0:
            print("[!] All accounts already at daily limit. Sleeping until tomorrow.\n")
        else:
            # ── Resume previous plan if one exists ─────────────────────────
            min_day = min(ws.get("day", 1) for ws in warmup_states.values())
            batches = None
            saved = load_batch_plan()
            if saved:
                reconciled = reconcile_plan(saved)
                if reconciled:
                    # Cap each batch size by remaining account capacity
                    total_in_plan = sum(b["size"] for b in reconciled)
                    if total_in_plan > total_remaining:
                        print(f"[resume] Plan had {total_in_plan} DMs but only {total_remaining} capacity left — trimming.")
                        # Trim from the tail
                        excess = total_in_plan - total_remaining
                        for b in reversed(reconciled):
                            take = min(b["size"], excess)
                            b["size"] -= take
                            excess   -= take
                            if excess <= 0:
                                break
                        reconciled = [b for b in reconciled if b["size"] > 0]
                    if reconciled:
                        batches = reconciled
                        print(f"[resume] Resuming plan with {len(batches)} batch(es), {sum(b['size'] for b in batches)} DM(s).")

            if batches is None:
                # Fresh plan
                clear_batch_plan()
                planned = plan_batches(total_remaining, warmup_day=min_day, proxy_tz=proxy_tz)
                batches = [{"start": t, "size": s, "done": 0} for t, s in planned]

                # Auto-shift first batch if far away — but ONLY when proxy-local
                # time is already inside active hours. If proxy is asleep (dead
                # zone or after 22:00), honor the planned wait.
                #
                # IMPORTANT: only shift batches that were originally planned for
                # TODAY (proxy-local). Next-day batches stay anchored to their
                # planned times — otherwise they end up at awkward early-morning
                # hours after the shift.
                if batches and (batches[0]["start"] - datetime.now()).total_seconds() > 600:
                    first_delay_min = int((batches[0]["start"] - datetime.now()).total_seconds() // 60)
                    now_proxy   = datetime.now(proxy_tz)
                    today_proxy = now_proxy.date()
                    in_active   = DEAD_ZONE[1] <= now_proxy.hour < 22

                    def _shift_today_only(batches, shift):
                        """Apply shift only to batches whose original date is today (proxy-local)."""
                        moved = 0
                        for b in batches:
                            b_local = b["start"].astimezone(proxy_tz) if b["start"].tzinfo else \
                                      b["start"].replace(tzinfo=datetime.now().astimezone().tzinfo).astimezone(proxy_tz)
                            if b_local.date() == today_proxy:
                                b["start"] = b["start"] + shift
                                moved += 1
                        return moved

                    if args.auto:
                        if in_active:
                            now   = datetime.now()
                            shift = (now + timedelta(seconds=random.randint(60, 180))) - batches[0]["start"]
                            moved = _shift_today_only(batches, shift)
                            kept  = len(batches) - moved
                            note  = f" ({kept} next-day batch{'es' if kept != 1 else ''} kept at original time)" if kept else ""
                            print(f"\n[*] Auto mode: first batch was {first_delay_min} min away — shifted "
                                  f"today's batches to start now{note} "
                                  f"(proxy-local {now_proxy.strftime('%H:%M')} is inside active hours).")
                        else:
                            reason = "dead zone" if now_proxy.hour < DEAD_ZONE[1] else "past active window"
                            print(f"\n[*] Auto mode: first batch is {first_delay_min} min away — NOT shifting "
                                  f"(proxy-local {now_proxy.strftime('%H:%M')} is in {reason}).")
                    else:
                        print(f"\n[*] First batch is {first_delay_min} min away. "
                              f"Proxy-local time: {now_proxy.strftime('%H:%M')} "
                              f"({'active' if in_active else 'asleep'}).")
                        if in_active:
                            start_now = input("Start first batch NOW instead? (y/N): ").strip().lower()
                            if start_now == "y":
                                now   = datetime.now()
                                shift = (now + timedelta(seconds=random.randint(60, 120))) - batches[0]["start"]
                                _shift_today_only(batches, shift)
                save_batch_plan(batches)

            print(f"── Today's Batch Plan ({len(batches)} batches) ──")
            for idx, b in enumerate(batches, 1):
                remaining = b["size"] - b["done"]
                # Show both system time and proxy-local time for clarity
                start_aware = b["start"].astimezone() if b["start"].tzinfo else \
                              b["start"].replace(tzinfo=datetime.now().astimezone().tzinfo)
                proxy_local = start_aware.astimezone(proxy_tz)
                print(f"  Batch {idx}: {remaining} DMs at "
                      f"{b['start'].strftime('%a %H:%M')} (server) / "
                      f"{proxy_local.strftime('%a %H:%M')} ({tz_label.split()[0]})"
                      + (f"  [done {b['done']}/{b['size']}]" if b["done"] else ""))
            print()

            # Execute batches
            for idx, b in enumerate(batches, 1):
                remaining = b["size"] - b["done"]
                if remaining <= 0:
                    continue
                if datetime.now() < b["start"]:
                    wait_until(b["start"], label=f"Batch {idx}/{len(batches)} ({remaining} DMs)")

                print(f"\n── Batch {idx}/{len(batches)} starting at {datetime.now().strftime('%H:%M')} ({remaining} DMs) ──")
                acc_index, sent, failed = run_batch(
                    remaining, pending, templates, accounts, warmup_states, clients, acc_index
                )
                grand_total  += sent
                grand_failed += failed
                b["done"]    += sent + failed
                save_batch_plan(batches)  # persist progress after every batch
                print(f"── Batch {idx} done: {sent} sent, {failed} failed ──")

                # If all accounts hit limit, no point continuing today
                if not any(can_send(a, warmup_states[a]) for a in accounts):
                    print("\n[!] All accounts hit daily limit. Skipping remaining batches today.")
                    break

                if not pending:
                    print("\n[✓] No more pending recipients!")
                    break

            # Day's plan exhausted — clear so tomorrow starts fresh
            clear_batch_plan()

        # ── Sleep until tomorrow's active window ──────────────────────────
        if not pending:
            print("\n[✓] All done. Exiting.")
            break

        # Sleep until tomorrow's first window IN THE PROXY'S LOCAL TIME.
        tomorrow_local = datetime.now(proxy_tz) + timedelta(days=1)
        first_ws = _windows_for_date(tomorrow_local.date())[0][0]
        tomorrow_aware = datetime(
            tomorrow_local.year, tomorrow_local.month, tomorrow_local.day,
            first_ws, random.randint(0, 30),
            random.randint(0, 59),
            tzinfo=proxy_tz,
        )
        tomorrow_start = tomorrow_aware.astimezone().replace(tzinfo=None)
        print(f"\n── Today's DMs complete ──")
        print(f"   Grand total sent: {grand_total}")
        print(f"   Grand total failed: {grand_failed}")
        print(f"   Sleeping until tomorrow {tomorrow_start.strftime('%Y-%m-%d %H:%M')}\n")

        wait_until(tomorrow_start, label="Next day's first batch")


if __name__ == "__main__":
    main()
