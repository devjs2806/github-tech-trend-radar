#!/usr/bin/env python3
"""
STEP C - Enrich the most interesting repos with metadata from the GitHub REST API.

GitHub events do NOT say which programming language a repo uses, so for the
repos that Job 1 found interesting (top starred, trending, emerging, health)
we call  GET /repos/{owner}/{repo}  and store language, topics, total stars,
creation date and licence in output/repo_meta.csv.

Already-fetched repos are cached, so re-running only fetches new ones.

Run AFTER job 1:
  export GITHUB_TOKEN=ghp_xxxx
  python3 enrich_metadata.py
"""
import csv
import os
import time

import requests

from config import GITHUB_TOKEN, MAX_REPOS_FOR_METADATA, OUTPUT_DIR
from utils import github_headers

SOURCES = ["trending_repos.csv", "top_starred_repos.csv", "emerging_repos.csv", "repo_health.csv"]
META_FILE = os.path.join(OUTPUT_DIR, "repo_meta.csv")
FIELDS = ["repo_name", "language", "topics", "total_stars", "total_forks",
          "created_at", "license", "description", "status"]


def wanted_repos():
    repos = []
    for name in SOURCES:
        path = os.path.join(OUTPUT_DIR, name)
        if not os.path.exists(path):
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                r = row.get("repo_name")
                if r and r not in repos:
                    repos.append(r)
    return repos[:MAX_REPOS_FOR_METADATA]


def load_cache():
    if not os.path.exists(META_FILE):
        return {}
    with open(META_FILE, newline="", encoding="utf-8") as f:
        return {row["repo_name"]: row for row in csv.DictReader(f)}


def fetch(session, repo):
    r = session.get(f"https://api.github.com/repos/{repo}", timeout=30)
    if r.status_code in (403, 429) and r.headers.get("X-RateLimit-Remaining") == "0":
        reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait = max(5, reset - int(time.time()) + 2)
        print(f"  rate limit reached, sleeping {wait}s ...")
        time.sleep(wait)
        r = session.get(f"https://api.github.com/repos/{repo}", timeout=30)
    if r.status_code == 401:
        raise SystemExit("GitHub says the token is invalid (HTTP 401). Check GITHUB_TOKEN in radar_env.sh")
    if r.status_code == 404 or r.status_code == 451:
        return {"repo_name": repo, "status": "not_found"}
    if r.status_code != 200:
        return None  # temporary problem: do not cache, try again next run
    d = r.json()
    return {
        "repo_name": repo,
        "language": d.get("language") or "Unknown",
        "topics": "|".join(d.get("topics") or []),
        "total_stars": d.get("stargazers_count"),
        "total_forks": d.get("forks_count"),
        "created_at": d.get("created_at"),
        "license": ((d.get("license") or {}).get("spdx_id")) or "",
        "description": (d.get("description") or "").replace("\n", " ")[:200],
        "status": "ok",
    }


def save(cache):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tmp = META_FILE + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for row in cache.values():
            w.writerow({k: row.get(k, "") for k in FIELDS})
    os.replace(tmp, META_FILE)


def main():
    repos = wanted_repos()
    if not repos:
        raise SystemExit("No result CSVs found in output/. Run job 1 first.")
    if not GITHUB_TOKEN:
        print("WARNING: no GITHUB_TOKEN -> only 60 API calls/hour. Set: export GITHUB_TOKEN=ghp_xxxx\n")

    cache = load_cache()
    todo = [r for r in repos if r not in cache]
    print(f"{len(repos)} interesting repos, {len(cache)} cached, {len(todo)} to fetch")

    session = requests.Session()
    session.headers.update(github_headers(GITHUB_TOKEN))
    for i, repo in enumerate(todo, 1):
        try:
            row = fetch(session, repo)
        except requests.RequestException as e:
            print(f"  {repo}: network error {e}")
            row = None
        if row:
            cache[repo] = row
        if i % 25 == 0 or i == len(todo):
            save(cache)
            print(f"  {i}/{len(todo)} fetched")
    save(cache)
    print(f"Done -> {META_FILE}")


if __name__ == "__main__":
    main()
