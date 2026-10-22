#!/usr/bin/env python3
"""
STEP A - Historical data capture.

Downloads hourly GH Archive files (every public GitHub event), keeps only the
fields we need, and drops them into the Flume spool folder
(~/github_spool/archive). Flume then ships them into HDFS.

Examples:
  python3 fetch_gharchive.py --days 3                  # last 3 full days (recommended)
  python3 fetch_gharchive.py --days 1 --hours 0-5      # quick 6-hour test
  python3 fetch_gharchive.py --start 2026-10-01 --end 2026-10-04
"""
import argparse
import gzip
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import requests

from config import SPOOL_ARCHIVE_DIR, STAGING_DIR
from utils import slim_event

BASE_URL = "https://data.gharchive.org/{d}-{h}.json.gz"


def parse_hours(text):
    if text == "all":
        return list(range(24))
    hours = set()
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-")
            hours.update(range(int(a), int(b) + 1))
        elif part:
            hours.add(int(part))
    return sorted(h for h in hours if 0 <= h <= 23)


def already_done(name):
    target = os.path.join(SPOOL_ARCHIVE_DIR, name)
    # Flume renames processed files to <name>.COMPLETED
    return os.path.exists(target) or os.path.exists(target + ".COMPLETED")


def fetch_hour(day, hour, retries=3):
    d = day.strftime("%Y-%m-%d")
    name = f"gharchive_{d}_{hour:02d}.json"
    if already_done(name):
        print(f"  {name}: already downloaded, skipping")
        return 0

    url = BASE_URL.format(d=d, h=hour)
    tmp = os.path.join(STAGING_DIR, name)

    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=(15, 300)) as r:
                if r.status_code == 404:
                    print(f"  {name}: not published yet (404), skipping")
                    return 0
                r.raise_for_status()
                r.raw.decode_content = False
                count = 0
                with gzip.GzipFile(fileobj=r.raw) as gz, open(tmp, "w", encoding="utf-8") as out:
                    for line in gz:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            ev = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        out.write(json.dumps(slim_event(ev)) + "\n")
                        count += 1
            os.replace(tmp, os.path.join(SPOOL_ARCHIVE_DIR, name))
            print(f"  {name}: {count:,} events")
            return count
        except (requests.RequestException, OSError, EOFError) as e:
            print(f"  {name}: attempt {attempt} failed ({e})")
            if os.path.exists(tmp):
                os.remove(tmp)
            time.sleep(5 * attempt)

    print(f"  {name}: FAILED after {retries} attempts")
    return 0


def main():
    p = argparse.ArgumentParser(description="Download GH Archive data into the Flume spool folder")
    p.add_argument("--days", type=int, default=2, help="number of days ending at --end (default 2)")
    p.add_argument("--start", help="start date YYYY-MM-DD (overrides --days)")
    p.add_argument("--end", help="end date YYYY-MM-DD (default: yesterday, UTC)")
    p.add_argument("--hours", default="all", help='hours to fetch, e.g. "all", "0-5", "0,6,12,18"')
    args = p.parse_args()

    os.makedirs(SPOOL_ARCHIVE_DIR, exist_ok=True)
    os.makedirs(STAGING_DIR, exist_ok=True)

    end = (date.fromisoformat(args.end) if args.end
           else datetime.now(timezone.utc).date() - timedelta(days=1))
    start = date.fromisoformat(args.start) if args.start else end - timedelta(days=args.days - 1)
    hours = parse_hours(args.hours)

    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    print(f"Downloading {len(days)} day(s) x {len(hours)} hour(s) from GH Archive")
    print(f"Output folder: {SPOOL_ARCHIVE_DIR}\n")

    total = 0
    for day in days:
        print(f"Day {day}")
        for h in hours:
            total += fetch_hour(day, h)

    print(f"\nDone. {total:,} events written to the spool folder.")


if __name__ == "__main__":
    main()
