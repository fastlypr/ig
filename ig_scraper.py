from ig_auth import get_client
import os
import csv
import time

INPUT_FILE = "usernames.txt"
OUTPUT_FILE = "results.csv"

CSV_HEADERS = [
    "Username", "Full Name", "User ID", "Bio", "Followers", "Following",
    "Posts", "Profile URL", "Is Private", "Is Verified", "Is Business",
    "Category", "Account Based In", "External URL", "Email (public)",
    "Phone (public)", "City", "Address", "Zip", "Latitude", "Longitude",
    "Profile Pic URL"
]




def get_account_based_in(cl, user_id):
    try:
        data = cl.with_default_data({"target_user_id": str(user_id)})
        result = cl.private_request(
            "bloks/apps/com.instagram.interactions.about_this_account/", data
        )
        for item in result["layout"]["bloks_payload"]["data"]:
            if item.get("data", {}).get("key") == "IG_ABOUT_THIS_ACCOUNT:about_this_account_country":
                return item["data"].get("initial", "N/A")
    except Exception:
        pass
    return "N/A"


def scrape_user(cl, username):
    try:
        user = cl.user_info_by_username_v1(username)
    except Exception as e:
        print(f"  [!] Error scraping @{username}: {e}")
        return None

    based_in = get_account_based_in(cl, user.pk)

    return {
        "Username": user.username,
        "Full Name": user.full_name,
        "User ID": user.pk,
        "Bio": user.biography,
        "Followers": user.follower_count,
        "Following": user.following_count,
        "Posts": user.media_count,
        "Profile URL": f"https://instagram.com/{user.username}",
        "Is Private": user.is_private,
        "Is Verified": user.is_verified,
        "Is Business": user.is_business,
        "Category": user.category or user.category_name or user.business_category_name or "",
        "Account Based In": based_in,
        "External URL": str(user.external_url) if user.external_url else "",
        "Email (public)": user.public_email or "",
        "Phone (public)": user.public_phone_number or "",
        "City": user.city_name or "",
        "Address": user.address_street or "",
        "Zip": user.zip or "",
        "Latitude": user.latitude or "",
        "Longitude": user.longitude or "",
        "Profile Pic URL": str(user.profile_pic_url),
    }


def load_usernames():
    if not os.path.exists(INPUT_FILE):
        print(f"[!] '{INPUT_FILE}' not found. Creating a sample file...")
        with open(INPUT_FILE, "w") as f:
            f.write("# Add one Instagram username per line\n")
            f.write("# Lines starting with # are ignored\n")
            f.write("viipan\n")
        print(f"[+] Created '{INPUT_FILE}'. Add usernames and run again.")
        exit()

    usernames = []
    with open(INPUT_FILE, "r") as f:
        for line in f:
            u = line.strip()
            if u and not u.startswith("#"):
                usernames.append(u)
    return usernames


# --- MAIN ---
cl = get_client()
usernames = load_usernames()

print(f"\n[+] Loaded {len(usernames)} usernames from '{INPUT_FILE}'")
print(f"[+] Results will be saved to '{OUTPUT_FILE}'\n")

file_exists = os.path.exists(OUTPUT_FILE)
with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
    if not file_exists:
        writer.writeheader()

    for i, username in enumerate(usernames, 1):
        print(f"[{i}/{len(usernames)}] Scraping @{username}...")
        data = scrape_user(cl, username)
        if data:
            writer.writerow(data)
            csvfile.flush()
            print(f"  [✓] Done — {data['Full Name']} | {data['Followers']} followers | Based In: {data['Account Based In']}")
        time.sleep(2)

print(f"\n[✓] All done! Results saved to '{OUTPUT_FILE}'")
