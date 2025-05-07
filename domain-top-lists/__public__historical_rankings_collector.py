import os
import datetime
import time
import requests
import zipfile
import gzip
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ---------------------------------------------------------------------
# 0) GLOBAL CONFIG & FOLDERS
# ---------------------------------------------------------------------
TIMEOUT = 60

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

BASE_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_FETCH_URL = "https://web.archive.org/web/"
UMBRELLA_BASE_URL = "http://s3-us-west-1.amazonaws.com/umbrella-static/"
TRANCO_LIST_DATE_URL = "https://tranco-list.eu/api/lists/date/{}"
CLOUDFLARE_BASE_URL = "https://api.cloudflare.com/client/v4/radar/datasets"

TRANCO_EMAIL = "PLACEHOLDER"
TRANCO_API_TOKEN = "PLACEHOLDER"
CLOUDFLARE_API_TOKEN = "PLACEHOLDER"

os.makedirs("historical_data/majestic", exist_ok=True)
os.makedirs("historical_data/umbrella", exist_ok=True)
os.makedirs("historical_data/tranco", exist_ok=True)
os.makedirs("historical_data/crux", exist_ok=True)
os.makedirs("historical_data/cloudflare", exist_ok=True)

TARGETS = {
    "majestic": "https://majestic.com/reports/majestic-million",
}

# ---------------------------------------------------------------------
global_cache = {
    "majestic": set(),
    "umbrella": set(),
    "tranco": set(),
    "crux": set(),
    "cloudflare": set()
}

# ---------------------------------------------------------------------
def init_cache_from_folders():
    for folder, prefix, ext in [
        ("historical_data/majestic", "majestic-", ".zip"),
        ("historical_data/umbrella", "umbrella-", ".csv.zip"),
        ("historical_data/tranco", "tranco-", ".zip"),
        ("historical_data/crux", "crux-", ".csv"),
        ("historical_data/cloudflare", "cloudflare-", ".csv")
    ]:
        if os.path.exists(folder):
            for fname in os.listdir(folder):
                if fname.endswith(ext):
                    date_str = fname.replace(prefix, "").replace(ext, "")
                    global_cache[folder.split("/")[-1]].add(date_str)

# ---------------------------------------------------------------------
def download_and_keep_both(url, csv_file_path, zip_file_path):
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    if resp.status_code != 200:
        print(f"[download_and_keep_both] HTTP {resp.status_code} error for URL: {url}")
        return False

    if url.endswith(".zip"):
        with open(zip_file_path, 'wb') as zf:
            zf.write(resp.content)
        print(f"Saved ZIP: {zip_file_path}")

        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zobj:
                zobj.extractall(os.path.dirname(csv_file_path))
            extracted = os.path.join(os.path.dirname(csv_file_path), "top-1m.csv")
            if os.path.exists(extracted):
                os.rename(extracted, csv_file_path)
            print(f"Extracted CSV: {csv_file_path}")
        except Exception as e:
            print(f"Error extracting from {zip_file_path}: {e}")
            return False
    else:
        with open(csv_file_path, 'wb') as f:
            f.write(resp.content)
        print(f"Saved CSV: {csv_file_path}")

        with zipfile.ZipFile(zip_file_path, mode='w', compression=zipfile.ZIP_DEFLATED) as z:
            z.write(csv_file_path, arcname=os.path.basename(csv_file_path))
        print(f"Created local ZIP: {zip_file_path}")

    return True

# ---------------------------------------------------------------------
def sanitize_date(date_str):
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str} (Expected YYYY-MM-DD)")

# ---------------------------------------------------------------------
def get_archived_urls(target_url):
    params = {"url": target_url, "output": "json", "fl": "timestamp,original"}
    resp = requests.get(BASE_URL, params=params, headers=HEADERS)
    if resp.status_code == 200:
        data = resp.json()
        if len(data) < 2:
            return []
        seen, unique_snapshots = set(), []
        for timestamp, orig in data[1:]:
            date_str = timestamp[:8]
            if date_str not in seen:
                seen.add(date_str)
                unique_snapshots.append((timestamp, orig))
        return unique_snapshots
    else:
        print(f"Wayback API error {resp.status_code} for {target_url}")
        return []

# ---------------------------------------------------------------------
def download_majestic_csv_for_dates(date_list, delay=3):
    snapshots = get_archived_urls(TARGETS["majestic"])
    if not snapshots:
        print("[Majestic] No snapshots found.")
        return

    filtered = {}
    for timestamp, archived_url in snapshots:
        day_str = datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%Y-%m-%d")
        if day_str in date_list and day_str not in global_cache["majestic"]:
            filtered[day_str] = (timestamp, archived_url)

    for day_str, (timestamp, archived_url) in filtered.items():
        wayback_page = f"{WAYBACK_FETCH_URL}{timestamp}/{archived_url}"
        print(f"[Majestic] Checking snapshot {wayback_page}")
        time.sleep(delay)

        try:
            page_resp = requests.get(wayback_page, headers=HEADERS, timeout=TIMEOUT)
            if page_resp.status_code == 200:
                soup = BeautifulSoup(page_resp.text, "html.parser")
                csv_links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".csv")]
                if csv_links:
                    csv_url = urljoin(wayback_page, csv_links[0])
                    csv_file_path = f"historical_data/majestic/majestic-{day_str}.csv"
                    zip_file_path = f"historical_data/majestic/majestic-{day_str}.zip"
                    time.sleep(delay)
                    print(f"[Majestic] Downloading CSV from {csv_url}")
                    success = download_and_keep_both(csv_url, csv_file_path, zip_file_path)
                    if success:
                        global_cache["majestic"].add(day_str)
        except requests.exceptions.RequestException as e:
            print(f"[Majestic] Error for {day_str}: {e}")

# ---------------------------------------------------------------------
def download_umbrella_csv_for_dates(date_list):
    for dstr in date_list:
        dstr = sanitize_date(dstr)
        if dstr in global_cache["umbrella"]:
            print(f"[Umbrella] Skipping {dstr}, already in cache.")
            continue

        zip_url = f"{UMBRELLA_BASE_URL}top-1m-{dstr}.csv.zip"
        csv_file_path = f"historical_data/umbrella/umbrella-{dstr}.csv"
        zip_file_path = f"historical_data/umbrella/umbrella-{dstr}.csv.zip"

        print(f"[Umbrella] Downloading ZIP from {zip_url}")
        success = download_and_keep_both(zip_url, csv_file_path, zip_file_path)
        if success:
            global_cache["umbrella"].add(dstr)

# ---------------------------------------------------------------------
def get_tranco_list_id(yyyymmdd):
    url = TRANCO_LIST_DATE_URL.format(yyyymmdd)
    resp = requests.get(url, auth=(TRANCO_EMAIL, TRANCO_API_TOKEN), headers=HEADERS, timeout=TIMEOUT)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("available"):
            return data["list_id"], data["download"]
    else:
        print(f"[Tranco] API error {resp.status_code} for date {yyyymmdd}")
    return None, None

# ---------------------------------------------------------------------
def download_tranco_csv_for_dates(date_list):
    for dstr in date_list:
        dstr = sanitize_date(dstr)
        if dstr in global_cache["tranco"]:
            print(f"[Tranco] Skipping {dstr}, already in cache.")
            continue

        yyyymmdd = datetime.datetime.strptime(dstr, "%Y-%m-%d").strftime("%Y%m%d")
        list_id, download_url = get_tranco_list_id(yyyymmdd)

        if list_id and download_url:
            csv_file_path = f"historical_data/tranco/tranco-{dstr}.csv"
            zip_file_path = f"historical_data/tranco/tranco-{dstr}.zip"
            print(f"[Tranco] Downloading list {list_id} for {dstr}")
            success = download_and_keep_both(download_url, csv_file_path, zip_file_path)
            if success:
                global_cache["tranco"].add(dstr)
        else:
            print(f"[Tranco] No list available for {dstr}.")

# ---------------------------------------------------------------------
# ---------------------------------------------------------------------
# CrUX Dataset Notes
# ---------------------------------------------------------------------
# The Chrome UX Report (CrUX) dataset is only updated monthly.
# Each file represents a full 28-day rolling average collected during
# the previous calendar month.
# For example:
# - Any date in July 2024 maps to the same monthly file: 202407.csv.gz
# Therefore, we deduplicate by converting each input date to its month (YYYYMM).

def download_crux_csv_for_dates(date_list):
    for dstr in date_list:
        try:
            dt_obj = datetime.datetime.strptime(dstr, "%Y-%m-%d")
            month_str = dt_obj.strftime("%Y%m")
        except ValueError:
            raise ValueError(f"Invalid date format: {dstr} (Expected YYYY-MM-DD)")

        if month_str in global_cache["crux"]:
            print(f"[CrUX] Skipping {month_str}, already in cache.")
            continue

        filename = f"{month_str}.csv.gz"
        url = f"https://raw.githubusercontent.com/zakird/crux-top-lists/main/data/global/{filename}"
        gz_file_path = f"historical_data/crux/crux-{month_str}.csv.gz"
        csv_file_path = f"historical_data/crux/crux-{month_str}.csv"

        print(f"[CrUX] Downloading gzip CSV from {url}")
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 200:
            with open(gz_file_path, "wb") as f:
                f.write(resp.content)
            print(f"Saved CrUX gzip: {gz_file_path}")

            with gzip.open(gz_file_path, 'rb') as f_in:
                with open(csv_file_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            print(f"Extracted CrUX CSV: {csv_file_path}")

            global_cache["crux"].add(month_str)
        else:
            print(f"[CrUX] No file found for {month_str} (HTTP {resp.status_code})")

# ---------------------------------------------------------------------
# ---------------------------------------------------------------------
# Cloudflare Radar Datasets
# ---------------------------------------------------------------------
# Cloudflare Radar datasets are provided on a **weekly basis**, not daily.
# Each dataset corresponds to one full week (Monday to Sunday), and dates
# are normalized to the **ISO week start**, which is Monday.
# For example:
# - 2024-07-22 (Monday) is Week 30
# - 2024-07-28 (Sunday) is also Week 30
# All of these dates will map to the same file.
# We deduplicate dates by converting them to the corresponding week-start date.

def download_cloudflare_csv_for_dates(date_list):
    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
        "Content-Type": "application/json"
    }

    for dstr in date_list:
        try:
            dt_obj = datetime.datetime.strptime(dstr, "%Y-%m-%d")
            week_start = dt_obj - datetime.timedelta(days=dt_obj.weekday())
            week_str = week_start.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {dstr} (Expected YYYY-MM-DD)")

        if week_str in global_cache["cloudflare"]:
            print(f"[Cloudflare] Skipping {week_str}, already in cache.")
            continue

        params = {
            "limit": 50,
            "datasetType": "RANKING_BUCKET"
        }
        response = requests.get(CLOUDFLARE_BASE_URL, headers=headers, params=params)
        if response.status_code != 200:
            print(f"[Cloudflare] Failed to retrieve datasets: {response.status_code}")
            continue

        datasets = response.json().get("result", {}).get("datasets", [])
        dataset_id = None
        for dataset in datasets:
            if dataset.get("meta", {}).get("top") == 1000000:
                dataset_id = dataset.get("id")
                break

        if not dataset_id:
            print(f"[Cloudflare] No dataset found for top 1,000,000 domains.")
            continue

        download_url = f"{CLOUDFLARE_BASE_URL}/download"
        response = requests.post(download_url, headers=headers, json={"datasetId": dataset_id})
        if response.status_code != 200:
            print(f"[Cloudflare] Failed to retrieve download URL: {response.status_code}")
            continue

        download_link = response.json().get("result", {}).get("dataset", {}).get("url")
        if not download_link:
            print(f"[Cloudflare] Download link not found.")
            continue

        csv_file_path = f"historical_data/cloudflare/cloudflare-{week_str}.csv"
        print(f"[Cloudflare] Downloading dataset from {download_link}")
        response = requests.get(download_link)
        if response.status_code == 200:
            with open(csv_file_path, 'wb') as f:
                f.write(response.content)
            print(f"Saved Cloudflare CSV: {csv_file_path}")
            global_cache["cloudflare"].add(week_str)
        else:
            print(f"[Cloudflare] Failed to download dataset: {response.status_code}")
# ---------------------------------------------------------------------
if __name__ == "__main__":
    print("Starting historical data collection for specific dates...")

    init_cache_from_folders()

    target_dates = ["2025-04-11", "2025-04-12", "2025-04-13", "2025-04-14", "2025-04-15", "2025-04-16", "2025-04-17", "2025-04-18", "2025-04-19", "2025-04-20", "2025-04-21"]

    download_majestic_csv_for_dates(target_dates)
    download_umbrella_csv_for_dates(target_dates)
    download_tranco_csv_for_dates(target_dates)
    download_crux_csv_for_dates(target_dates)
    download_cloudflare_csv_for_dates(target_dates)
