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
import csv
import json
import time
import random
import requests
import io
from datetime import date, datetime, timedelta
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

# Delay between DMs (seconds)
DELAY_MIN    = 180   # 3 min
DELAY_MAX    = 300   # 5 min
DELAY_JITTER = 15    # ±15 sec jitter

# Rate limit pause
RATE_LIMIT_PAUSE_MIN = 900    # 15 min
RATE_LIMIT_PAUSE_MAX = 1800   # 30 min

# Active hours (24h format) — DMs only sent inside this window
ACTIVE_START_HOUR = 9    # 9 AM
ACTIVE_END_HOUR   = 22   # 10 PM

# Batch scheduling (spreads daily DMs across the day)
BATCH_SIZE_MIN = 2     # min DMs per batch
BATCH_SIZE_MAX = 6     # max DMs per batch


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
        "last_run_date": str(date.today()),
        "total_sent":    0,
    }


def save_warmup(account, state):
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    with open(_warmup_file(account), "w") as f:
        json.dump(state, f, indent=2)


def update_warmup_day(account, state):
    """If it's a new calendar day, reset daily counter and increase limit."""
    today = str(date.today())
    if state["last_run_date"] != today:
        growth    = random.randint(WARMUP_GROWTH_MIN, WARMUP_GROWTH_MAX)
        new_limit = min(state["daily_limit"] + growth, WARMUP_MAX)
        print(f"  [warmup] New day for @{account}! Limit: {state['daily_limit']} → {new_limit} DMs/day")
        state["day"]           += 1
        state["daily_sent"]    = 0
        state["daily_limit"]   = new_limit
        state["last_run_date"] = today
        save_warmup(account, state)
    return state


def can_send(account, state):
    return state["daily_sent"] < state["daily_limit"]


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


def human_delay():
    base  = random.randint(DELAY_MIN, DELAY_MAX)
    total = max(60, base + random.randint(-DELAY_JITTER, DELAY_JITTER))
    print(f"  [delay] Waiting {total // 60}m {total % 60}s before next DM...")
    time.sleep(total)


def rate_limit_pause():
    pause = random.randint(RATE_LIMIT_PAUSE_MIN, RATE_LIMIT_PAUSE_MAX)
    print(f"\n  [!] Rate limit hit. Pausing {pause // 60} minutes...")
    time.sleep(pause)


# ── Batch Scheduler ───────────────────────────────────────────────────────────

def plan_batches(total_limit, start_from=None):
    """
    Split today's DM limit into randomized batches spread across the active
    window. Returns a list of (start_datetime, batch_size) tuples.
    """
    if total_limit <= 0:
        return []

    # Batch count scales with size
    if total_limit <= 4:
        batch_count = 1
    elif total_limit <= 10:
        batch_count = random.randint(2, 3)
    elif total_limit <= 20:
        batch_count = random.randint(3, 4)
    else:
        batch_count = random.randint(4, 6)

    # Split total into that many random-sized pieces
    sizes = [0] * batch_count
    for _ in range(total_limit):
        sizes[random.randint(0, batch_count - 1)] += 1
    sizes = [s for s in sizes if s > 0]  # drop empties
    batch_count = len(sizes)

    # Active window for today (or tomorrow if we're already past it)
    now = start_from or datetime.now()
    today_start = now.replace(hour=ACTIVE_START_HOUR, minute=0, second=0, microsecond=0)
    today_end   = now.replace(hour=ACTIVE_END_HOUR,   minute=0, second=0, microsecond=0)

    if now >= today_end:
        # Past active window — schedule for tomorrow
        today_start += timedelta(days=1)
        today_end   += timedelta(days=1)
    elif now > today_start:
        # Mid-day start — use now as window start
        today_start = now + timedelta(seconds=random.randint(30, 180))

    window_seconds = int((today_end - today_start).total_seconds())
    if window_seconds < 60:
        # No room left today — push to tomorrow
        today_start = (today_start + timedelta(days=1)).replace(hour=ACTIVE_START_HOUR, minute=0, second=0, microsecond=0)
        today_end   = today_start.replace(hour=ACTIVE_END_HOUR)
        window_seconds = int((today_end - today_start).total_seconds())

    # Divide window into equal segments, pick a random time within each segment
    segment = window_seconds // batch_count
    batch_plan = []
    for i, size in enumerate(sizes):
        seg_start = i * segment
        seg_end   = max(seg_start + 1, (i + 1) * segment)
        offset    = random.randint(seg_start, seg_end - 1)
        batch_plan.append((today_start + timedelta(seconds=offset), size))

    return batch_plan


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
    """Send a batch of DMs with 3-5 min gaps between each."""
    sent_in_batch = 0
    failed_in_batch = 0

    for i in range(batch_size):
        if not pending:
            break

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
            human_delay()

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
    print("\n── ig_dm — Instagram DM Sender (All-Day Mode) ──\n")

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

    # ── Account picker ────────────────────────────────────────────────────
    accounts = pick_accounts(all_accounts, cfg_data)
    if not accounts:
        print("[!] No accounts selected. Exiting.")
        return

    clients       = {}
    acc_index     = 0
    grand_total   = 0
    grand_failed  = 0

    print(f"\n[+] Using {len(accounts)} account(s): {', '.join('@'+a for a in accounts)}")
    print(f"[+] Active hours: {ACTIVE_START_HOUR:02d}:00 – {ACTIVE_END_HOUR:02d}:00")
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

        # Today's total capacity = sum of remaining limits across all accounts
        total_remaining = sum(
            max(0, ws["daily_limit"] - ws["daily_sent"])
            for ws in warmup_states.values()
        )
        total_remaining = min(total_remaining, len(pending))

        print("── Account Warmup Status ──")
        for acc in accounts:
            ws = warmup_states[acc]
            print(f"  @{acc} — Day {ws['day']} | {ws['daily_sent']}/{ws['daily_limit']} today | Total: {ws['total_sent']}")
        print(f"\n[+] Total DMs to send today: {total_remaining}\n")

        if total_remaining == 0:
            print("[!] All accounts already at daily limit. Sleeping until tomorrow.\n")
        else:
            # Plan batches for today
            batches = plan_batches(total_remaining)
            print(f"── Today's Batch Plan ({len(batches)} batches) ──")
            for idx, (start_time, size) in enumerate(batches, 1):
                print(f"  Batch {idx}: {size} DMs at {start_time.strftime('%H:%M')}")

            # Offer to start first batch immediately (for testing / first-run)
            if batches and (batches[0][0] - datetime.now()).total_seconds() > 600:
                first_delay_min = int((batches[0][0] - datetime.now()).total_seconds() // 60)
                print(f"\n[*] First batch is {first_delay_min} min away.")
                start_now = input("Start first batch NOW instead? (y/N): ").strip().lower()
                if start_now == "y":
                    now   = datetime.now()
                    shift = (now + timedelta(seconds=random.randint(60, 120))) - batches[0][0]
                    batches = [(t + shift, s) for t, s in batches]
                    print("\n── Updated Batch Plan ──")
                    for idx, (start_time, size) in enumerate(batches, 1):
                        print(f"  Batch {idx}: {size} DMs at {start_time.strftime('%H:%M')}")
            print()

            # Execute batches
            for idx, (start_time, size) in enumerate(batches, 1):
                if datetime.now() < start_time:
                    wait_until(start_time, label=f"Batch {idx}/{len(batches)} ({size} DMs)")

                print(f"\n── Batch {idx}/{len(batches)} starting at {datetime.now().strftime('%H:%M')} ({size} DMs) ──")
                acc_index, sent, failed = run_batch(
                    size, pending, templates, accounts, warmup_states, clients, acc_index
                )
                grand_total  += sent
                grand_failed += failed
                print(f"── Batch {idx} done: {sent} sent, {failed} failed ──")

                # If all accounts hit limit, no point continuing today
                if not any(can_send(a, warmup_states[a]) for a in accounts):
                    print("\n[!] All accounts hit daily limit. Skipping remaining batches today.")
                    break

                if not pending:
                    print("\n[✓] No more pending recipients!")
                    break

        # ── Sleep until tomorrow's active window ──────────────────────────
        if not pending:
            print("\n[✓] All done. Exiting.")
            break

        tomorrow_start = (datetime.now() + timedelta(days=1)).replace(
            hour=ACTIVE_START_HOUR,
            minute=random.randint(0, 30),  # random start time to avoid pattern
            second=random.randint(0, 59),
            microsecond=0,
        )
        print(f"\n── Today's DMs complete ──")
        print(f"   Grand total sent: {grand_total}")
        print(f"   Grand total failed: {grand_failed}")
        print(f"   Sleeping until tomorrow {tomorrow_start.strftime('%Y-%m-%d %H:%M')}\n")

        wait_until(tomorrow_start, label="Next day's first batch")


if __name__ == "__main__":
    main()
