"""
ig_auth.py — Centralized Instagram account manager.
Import get_client() in any script to get a ready-to-use logged-in Client.
Run directly to manage accounts interactively.
"""

from instagrapi import Client
from instagrapi.exceptions import ChallengeRequired, LoginRequired
import os
import json
import uuid
import random
import getpass
from datetime import date

ACCOUNTS_FILE = "accounts.json"
SESSIONS_DIR  = "sessions"

DEVICE_POOL = [
    {"manufacturer": "OnePlus",  "model": "6T Dev",       "device": "devitron",       "cpu": "qcom",       "android_version": 28, "android_release": "9.0.0",  "dpi": "480dpi", "resolution": "1080x2340"},
    {"manufacturer": "OnePlus",  "model": "8 Pro",         "device": "instantnoodlep", "cpu": "qcom",       "android_version": 30, "android_release": "11.0.0", "dpi": "560dpi", "resolution": "1440x3168"},
    {"manufacturer": "samsung",  "model": "SM-G973F",      "device": "beyond1",        "cpu": "exynos9820", "android_version": 29, "android_release": "10.0.0", "dpi": "550dpi", "resolution": "1440x3040"},
    {"manufacturer": "samsung",  "model": "SM-G991B",      "device": "o1s",            "cpu": "exynos2100", "android_version": 31, "android_release": "12.0.0", "dpi": "421dpi", "resolution": "1080x2400"},
    {"manufacturer": "Xiaomi",   "model": "MI 9",          "device": "cepheus",        "cpu": "qcom",       "android_version": 28, "android_release": "9.0.0",  "dpi": "440dpi", "resolution": "1080x2340"},
    {"manufacturer": "Xiaomi",   "model": "Redmi Note 8",  "device": "ginkgo",         "cpu": "qcom",       "android_version": 29, "android_release": "10.0.0", "dpi": "395dpi", "resolution": "1080x2340"},
]


# ── accounts.json helpers ─────────────────────────────────────────────────────

def _load_accounts_file():
    if not os.path.exists(ACCOUNTS_FILE):
        return {"default": None, "proxy_pool": [], "accounts": {}}
    with open(ACCOUNTS_FILE, "r") as f:
        data = json.load(f)
    # ── Migrate old format: global proxy → proxy_pool ──────────────────────
    if "proxy" in data and "proxy_pool" not in data:
        old_proxy = data.pop("proxy")
        data["proxy_pool"] = [old_proxy] if old_proxy else []
        # Move global proxy into each existing account that has none
        for acc_info in data.get("accounts", {}).values():
            if "proxy" not in acc_info:
                acc_info["proxy"] = old_proxy
        _save_accounts_file(data)
    if "proxy_pool" not in data:
        data["proxy_pool"] = []
    return data


def _save_accounts_file(data):
    with open(ACCOUNTS_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ── Device fingerprint ────────────────────────────────────────────────────────

def _generate_device_fingerprint():
    device = random.choice(DEVICE_POOL)
    return {
        "uuids": {
            "phone_id":          str(uuid.uuid4()),
            "uuid":              str(uuid.uuid4()),
            "client_session_id": str(uuid.uuid4()),
            "advertising_id":    str(uuid.uuid4()),
            "device_id":         "android-" + uuid.uuid4().hex[:16],
        },
        "device_settings": {
            "android_version":  device["android_version"],
            "android_release":  device["android_release"],
            "dpi":              device["dpi"],
            "resolution":       device["resolution"],
            "manufacturer":     device["manufacturer"],
            "device":           device["device"],
            "model":            device["model"],
            "cpu":              device["cpu"],
        },
    }


def _save_device(device_data, device_file):
    with open(device_file, "w") as f:
        json.dump(device_data, f, indent=2)


# ── Login helpers ─────────────────────────────────────────────────────────────

def _handle_challenge(cl, username, password):
    challenge_url = cl.last_json.get("challenge", {}).get("url", "")
    print(f"\n[!] Instagram requires verification for @{username}")
    print(f"[!] Open this URL in your browser:\n\n    {challenge_url}\n")
    input("Press ENTER after completing the verification in browser...")
    cl.login(username, password)


def _build_client(device_file, session_file, proxy):
    cl = Client(proxy=proxy)
    if os.path.exists(device_file):
        cl.load_settings(device_file)
    if os.path.exists(session_file):
        cl.load_settings(session_file)
    return cl


def _do_login(cl, username, password, session_file):
    try:
        cl.login(username, password)
    except ChallengeRequired:
        _handle_challenge(cl, username, password)
    cl.dump_settings(session_file)


# ── Proxy Pool helpers ────────────────────────────────────────────────────────

def _next_unassigned_proxy(cfg_data):
    """Return the first proxy from pool not yet assigned to any account."""
    pool = cfg_data.get("proxy_pool", [])
    used = {info.get("proxy") for info in cfg_data.get("accounts", {}).values() if info.get("proxy")}
    for proxy in pool:
        if proxy not in used:
            return proxy
    return None


def _proxy_usage(cfg_data):
    """Return dict: proxy → list of account usernames using it."""
    usage = {}
    for acc, info in cfg_data.get("accounts", {}).items():
        p = info.get("proxy") or "No proxy"
        usage.setdefault(p, []).append(acc)
    return usage


# ── Public API ────────────────────────────────────────────────────────────────

def get_client(account: str = None, proxy: str = None) -> Client:
    """
    Returns a ready-to-use logged-in instagrapi Client.

    Usage:
        from ig_auth import get_client
        cl = get_client()                   # uses default account
        cl = get_client("other_account")    # specific account
        cl = get_client(proxy=None)         # override proxy to none
    """
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    cfg_data = _load_accounts_file()

    if not cfg_data["accounts"]:
        raise RuntimeError("No accounts configured. Run `python ig_auth.py` to add one.")

    username = account or cfg_data.get("default")
    if not username or username not in cfg_data["accounts"]:
        raise ValueError(f"Account '{username}' not found. Run `python ig_auth.py` to manage accounts.")

    acc          = cfg_data["accounts"][username]
    password     = acc["password"]
    session_file = acc["session_file"]
    device_file  = acc["device_file"]
    # Per-account proxy — override only if caller explicitly passes one
    use_proxy    = proxy if proxy is not None else acc.get("proxy")

    cl = _build_client(device_file, session_file, use_proxy)

    if os.path.exists(session_file):
        try:
            cl.login(username, password)
            print(f"[+] Logged in via saved session as @{cl.username}")
            return cl
        except (LoginRequired, Exception) as e:
            print(f"[!] Session expired for @{username} ({e}). Re-logging in...")

    _do_login(cl, username, password, session_file)
    print(f"[+] Logged in as @{cl.username}")
    return cl


# ── Interactive menu ──────────────────────────────────────────────────────────

def _add_account():
    cfg_data = _load_accounts_file()
    os.makedirs(SESSIONS_DIR, exist_ok=True)

    print("\n── Add New Account ──")
    username = input("Instagram username: ").strip().lower()
    if not username:
        print("[!] Username cannot be empty.")
        return

    if username in cfg_data["accounts"]:
        print(f"[!] @{username} already exists.")
        return

    password = getpass.getpass("Password (hidden): ")
    if not password:
        print("[!] Password cannot be empty.")
        return

    # ── Proxy assignment ──────────────────────────────────────────────────
    proxy = None
    use_proxy = input("\nUse a proxy for this account? (y/n): ").strip().lower()

    if use_proxy == "y":
        pool       = cfg_data.get("proxy_pool", [])
        auto_proxy = _next_unassigned_proxy(cfg_data)

        if auto_proxy:
            # Pool has a free proxy — auto-assign it, no need to prompt
            proxy = auto_proxy
            print(f"[+] Auto-assigned proxy from pool: {proxy}")
        else:
            # No free proxy — ask for a new one
            if pool:
                print("[!] All proxies in pool are already in use by other accounts.")
            new_proxy = input("Enter proxy URL (http://user:pass@host:port): ").strip()
            if new_proxy:
                proxy = new_proxy
                cfg_data["proxy_pool"].append(proxy)
                print(f"[+] Added to pool and assigned: {proxy}")
            else:
                print("[!] No proxy entered — continuing without proxy.")
    else:
        print("[*] Skipping proxy — account will connect directly.")

    session_file = f"{SESSIONS_DIR}/{username}_session.json"
    device_file  = f"{SESSIONS_DIR}/{username}_device.json"

    print(f"\n[*] Generating unique device fingerprint for @{username}...")
    device_data = _generate_device_fingerprint()
    _save_device(device_data, device_file)
    dev = device_data["device_settings"]
    print(f"[+] Device: {dev['manufacturer']} {dev['model']} (Android {dev['android_release']})")

    if proxy:
        print(f"[*] Using proxy: {proxy}")
    else:
        print("[*] No proxy assigned.")

    print(f"[*] Testing login for @{username}...")
    cl = Client(proxy=proxy)
    cl.load_settings(device_file)

    try:
        try:
            cl.login(username, password)
        except ChallengeRequired:
            _handle_challenge(cl, username, password)

        cl.dump_settings(session_file)
        print(f"[✓] Login successful for @{username}!")

    except Exception as e:
        print(f"[!] Login failed: {e}")
        os.remove(device_file)
        return

    cfg_data["accounts"][username] = {
        "password":     password,
        "session_file": session_file,
        "device_file":  device_file,
        "proxy":        proxy,
        "added_at":     str(date.today()),
    }

    if not cfg_data.get("default"):
        cfg_data["default"] = username
        print(f"[+] Set @{username} as default account.")

    _save_accounts_file(cfg_data)
    print(f"[✓] @{username} saved to {ACCOUNTS_FILE}")


def _list_accounts():
    cfg_data = _load_accounts_file()
    accounts = cfg_data.get("accounts", {})

    if not accounts:
        print("\n[!] No accounts configured yet.")
        return

    default = cfg_data.get("default")
    print(f"\n── Accounts ({len(accounts)}) ──")
    for acc, info in accounts.items():
        session_ok = os.path.exists(info["session_file"])
        device_ok  = os.path.exists(info["device_file"])
        tag        = " [default]" if acc == default else ""
        session    = "Session OK" if session_ok else "No session"
        device     = "Device OK"  if device_ok  else "No device"
        proxy      = info.get("proxy") or "No proxy"
        print(f"  @{acc}{tag} — {session} | {device} | Proxy: {proxy} | Added: {info.get('added_at', 'N/A')}")

    pool = cfg_data.get("proxy_pool", [])
    print(f"\n  Proxy pool: {len(pool)} proxies")


def _remove_account():
    cfg_data = _load_accounts_file()
    accounts = cfg_data.get("accounts", {})

    if not accounts:
        print("\n[!] No accounts to remove.")
        return

    _list_accounts()
    username = input("\nEnter username to remove: ").strip().lower()

    if username not in accounts:
        print(f"[!] @{username} not found.")
        return

    confirm = input(f"Remove @{username}? This deletes session files too. (y/n): ").strip().lower()
    if confirm != "y":
        print("[!] Cancelled.")
        return

    info = accounts.pop(username)
    for f in [info["session_file"], info["device_file"]]:
        if os.path.exists(f):
            os.remove(f)

    if cfg_data.get("default") == username:
        cfg_data["default"] = next(iter(accounts), None)
        print(f"[+] Default changed to @{cfg_data['default']}.")

    _save_accounts_file(cfg_data)
    print(f"[✓] @{username} removed.")


def _set_default():
    cfg_data = _load_accounts_file()
    accounts = cfg_data.get("accounts", {})

    if not accounts:
        print("\n[!] No accounts configured.")
        return

    _list_accounts()
    username = input("\nEnter username to set as default: ").strip().lower()

    if username not in accounts:
        print(f"[!] @{username} not found.")
        return

    cfg_data["default"] = username
    _save_accounts_file(cfg_data)
    print(f"[✓] @{username} is now the default account.")


def _test_login():
    cfg_data = _load_accounts_file()
    accounts = cfg_data.get("accounts", {})

    if not accounts:
        print("\n[!] No accounts configured.")
        return

    _list_accounts()
    username = input("\nEnter username to test: ").strip().lower()

    if username not in accounts:
        print(f"[!] @{username} not found.")
        return

    print(f"\n[*] Testing login for @{username}...")
    try:
        cl = get_client(username)
        proxy = cfg_data["accounts"][username].get("proxy") or "No proxy"
        print(f"[✓] Success! Logged in as @{cl.username} (ID: {cl.user_id})")
        print(f"    Proxy: {proxy}")
    except Exception as e:
        print(f"[!] Login failed: {e}")


def _manage_proxies():
    cfg_data = _load_accounts_file()
    pool     = cfg_data.get("proxy_pool", [])
    usage    = _proxy_usage(cfg_data)

    while True:
        print(f"\n── Proxy Pool ({len(pool)} proxies) ──")
        if pool:
            for idx, p in enumerate(pool, 1):
                accs = usage.get(p, [])
                tag  = f"  ← {', '.join('@'+a for a in accs)}" if accs else "  ← unassigned"
                print(f"  {idx}. {p}{tag}")
        else:
            print("  (empty)")

        print("\n  a. Add proxy")
        print("  r. Remove proxy")
        print("  c. Change proxy for an account")
        print("  b. Back")

        choice = input("\nChoice: ").strip().lower()

        if choice == "a":
            new_proxy = input("Proxy URL (e.g. http://user:pass@host:port): ").strip()
            if not new_proxy:
                print("[!] Empty input.")
            elif new_proxy in pool:
                print("[!] Proxy already in pool.")
            else:
                pool.append(new_proxy)
                cfg_data["proxy_pool"] = pool
                _save_accounts_file(cfg_data)
                print(f"[✓] Added: {new_proxy}")

        elif choice == "r":
            if not pool:
                print("[!] Pool is empty.")
                continue
            num = input("Enter proxy number to remove: ").strip()
            if not num.isdigit() or not (1 <= int(num) <= len(pool)):
                print("[!] Invalid number.")
                continue
            removed = pool.pop(int(num) - 1)
            # Clear from any accounts using it
            for acc_info in cfg_data["accounts"].values():
                if acc_info.get("proxy") == removed:
                    acc_info["proxy"] = None
            cfg_data["proxy_pool"] = pool
            _save_accounts_file(cfg_data)
            print(f"[✓] Removed: {removed}")
            print("[!] Accounts using this proxy now have no proxy assigned.")

        elif choice == "c":
            accounts = cfg_data.get("accounts", {})
            if not accounts:
                print("[!] No accounts configured.")
                continue
            _list_accounts()
            username = input("\nEnter username to update: ").strip().lower()
            if username not in accounts:
                print(f"[!] @{username} not found.")
                continue

            print("\nPick a proxy:")
            for idx, p in enumerate(pool, 1):
                accs = usage.get(p, [])
                tag  = f"  ← {', '.join('@'+a for a in accs)}" if accs else "  ← unassigned"
                print(f"  {idx}. {p}{tag}")
            print("  0. Remove proxy (no proxy)")

            num = input("Choice: ").strip()
            if num == "0":
                cfg_data["accounts"][username]["proxy"] = None
                _save_accounts_file(cfg_data)
                print(f"[✓] Removed proxy from @{username}.")
            elif num.isdigit() and 1 <= int(num) <= len(pool):
                new_p = pool[int(num) - 1]
                cfg_data["accounts"][username]["proxy"] = new_p
                _save_accounts_file(cfg_data)
                print(f"[✓] @{username} → {new_p}")
            else:
                print("[!] Invalid choice.")

            # Refresh usage map
            usage = _proxy_usage(cfg_data)

        elif choice == "b":
            break
        else:
            print("[!] Invalid choice.")


def _main_menu():
    cfg_data = _load_accounts_file()

    # First time setup
    if not cfg_data["accounts"]:
        print("\n Welcome to ig_auth — Instagram Account Manager")
        print("─" * 50)
        print("[!] No accounts found. Let's add your first account.\n")
        _add_account()
        return

    while True:
        print("\n── ig_auth Menu ──")
        print("  1. Add new account")
        print("  2. List accounts")
        print("  3. Remove account")
        print("  4. Test login")
        print("  5. Set default account")
        print("  6. Manage proxies")
        print("  7. Exit")

        choice = input("\nChoice: ").strip()
        if choice == "1":
            _add_account()
        elif choice == "2":
            _list_accounts()
        elif choice == "3":
            _remove_account()
        elif choice == "4":
            _test_login()
        elif choice == "5":
            _set_default()
        elif choice == "6":
            _manage_proxies()
        elif choice == "7":
            break
        else:
            print("[!] Invalid choice.")


if __name__ == "__main__":
    _main_menu()
