#!/usr/bin/env python3
"""
STEP B - Live data capture (streaming source).

Polls the GitHub public Events API every LIVE_POLL_SECONDS, removes
duplicates, and drops a new JSON-lines file into ~/github_spool/live.
Flume picks each file up and streams it into HDFS /github/raw/live.

Run it and leave it running (Ctrl+C to stop):
  export GITHUB_TOKEN=ghp_xxxx
  python3 fetch_live_events.py
"""
import time
from collections import deque
from datetime import datetime, timezone

import requests

from config import GITHUB_TOKEN, LIVE_POLL_SECONDS, SPOOL_LIVE_DIR, STAGING_DIR
from utils import github_headers, slim_event, write_jsonl_atomically

URL = "https://api.github.com/events"


def now():
    return datetime.now().strftime("%H:%M:%S")


def main():
    if GITHUB_TOKEN:
        pages, interval = 3, LIVE_POLL_SECONDS
    else:
        print("WARNING: GITHUB_TOKEN is not set. Without a token GitHub allows only 60 calls/hour,")
        print("         so polling will be slower. Set it with: export GITHUB_TOKEN=ghp_xxxx\n")
        pages, interval = 1, max(LIVE_POLL_SECONDS, 90)

    session = requests.Session()
    session.headers.update(github_headers(GITHUB_TOKEN))

    seen_order = deque(maxlen=20000)
    seen = set()
    print(f"Polling {URL} every {interval}s -> {SPOOL_LIVE_DIR}  (Ctrl+C to stop)\n")

    while True:
        new = []
        for page in range(1, pages + 1):
            try:
                r = session.get(URL, params={"per_page": 100, "page": page}, timeout=30)
            except requests.RequestException as e:
                print(f"[{now()}] network error: {e}")
                break

            if r.status_code == 429 or (r.status_code == 403 and r.headers.get("X-RateLimit-Remaining") == "0"):
                reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait = max(10, reset - int(time.time()))
                print(f"[{now()}] rate limited, sleeping {wait}s")
                time.sleep(wait)
                break
            if r.status_code == 401:
                raise SystemExit("GitHub says the token is invalid (HTTP 401). Check GITHUB_TOKEN in radar_env.sh")
            if r.status_code != 200:
                print(f"[{now()}] GitHub returned HTTP {r.status_code}: {r.text[:150]}")
                break

            for ev in r.json():
                eid = ev.get("id")
                if not eid or eid in seen:
                    continue
                if len(seen_order) == seen_order.maxlen:
                    seen.discard(seen_order[0])
                seen_order.append(eid)
                seen.add(eid)
                new.append(slim_event(ev))

        if new:
            name = f"live_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json"
            write_jsonl_atomically(new, name, SPOOL_LIVE_DIR, STAGING_DIR)
            print(f"[{now()}] {len(new):4d} new events -> {name}")
        else:
            print(f"[{now()}] no new events")

        time.sleep(interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
