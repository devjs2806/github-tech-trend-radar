#!/usr/bin/env python3
"""
OFFLINE TEST DATA (optional).

Creates realistic *synthetic* GitHub events in the same format as the real
fetchers, so you can test Flume -> HDFS -> Spark without internet.
Use REAL data (fetch_gharchive.py) for your final project results.

  python3 tools/make_sample_data.py --hours 48            # -> ~/github_spool/archive
  python3 tools/make_sample_data.py --live --hours 1      # -> ~/github_spool/live
"""
import argparse
import os
import random
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SPOOL_ARCHIVE_DIR, SPOOL_LIVE_DIR, STAGING_DIR  # noqa: E402
from utils import write_jsonl_atomically  # noqa: E402

TYPES = [("PushEvent", 45), ("WatchEvent", 12), ("CreateEvent", 12), ("PullRequestEvent", 9),
         ("IssueCommentEvent", 6), ("IssuesEvent", 4), ("DeleteEvent", 4), ("ForkEvent", 3),
         ("PullRequestReviewEvent", 3), ("ReleaseEvent", 1), ("PublicEvent", 1)]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--hours", type=int, default=48)
    p.add_argument("--events-per-hour", type=int, default=6000)
    p.add_argument("--repos", type=int, default=3000)
    p.add_argument("--live", action="store_true", help="write to the live spool folder, ending now")
    p.add_argument("--seed", type=int, default=7)
    a = p.parse_args()
    rnd = random.Random(a.seed)

    orgs = [f"org{i}" for i in range(150)]
    repos = [f"{rnd.choice(orgs) if rnd.random() < .5 else 'user' + str(rnd.randint(0, 9999))}/project-{i}"
             for i in range(a.repos)]
    weights = [1 / (i + 1) ** 0.9 for i in range(a.repos)]            # long-tail popularity
    viral = rnd.sample(repos[100:], 8)                                 # repos that take off
    last = max(a.hours - 1, 0)
    viral_start = {r: rnd.randint(min(a.hours // 3, last), max(a.hours - 6, min(a.hours // 3, last))) for r in viral}
    new_repos = [f"newdev{i}/ai-agent-{i}" for i in range(10)]
    new_start = {r: rnd.randint(0, max(a.hours - 4, 0)) for r in new_repos}
    users = [f"dev{i}" for i in range(40000)] + ["dependabot[bot]", "github-actions[bot]", "renovate[bot]"]
    type_names = [t for t, _ in TYPES]
    type_w = [w for _, w in TYPES]

    end = (datetime.now(timezone.utc).replace(second=0, microsecond=0)
           if a.live else datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0))
    start = end - timedelta(hours=a.hours)
    target = SPOOL_LIVE_DIR if a.live else SPOOL_ARCHIVE_DIR
    eid = rnd.randint(10**10, 10**11)
    total = 0

    for h in range(a.hours):
        hour_start = start + timedelta(hours=h)
        daily = 0.6 + 0.4 * abs(((hour_start.hour + 5) % 24) - 12) / 12   # day/night cycle
        n = int(a.events_per_hour * daily)
        recs = []
        for _ in range(n):
            t = rnd.choices(type_names, type_w)[0]
            repo = rnd.choices(repos, weights)[0]
            if t == "WatchEvent":
                hot = [r for r in viral if h >= viral_start[r]] + [r for r in new_repos if h >= new_start[r]]
                if hot and rnd.random() < 0.35:
                    repo = rnd.choice(hot)
            user = rnd.choice(users[-3:]) if (t in ("PushEvent", "PullRequestEvent") and rnd.random() < .15) \
                else rnd.choice(users[:-3])
            ts = hour_start + timedelta(seconds=rnd.randint(0, 3599))
            eid += 1
            recs.append({"id": str(eid), "type": t, "created_at": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "actor_login": user, "repo_name": repo,
                         "org_login": repo.split("/")[0] if repo.startswith("org") else None,
                         "action": "started" if t == "WatchEvent" else None,
                         "ref_type": "branch" if t == "CreateEvent" else None})
        for r in new_repos:  # creation event for the brand-new repos
            if new_start[r] == h:
                eid += 1
                recs.append({"id": str(eid), "type": "CreateEvent",
                             "created_at": hour_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                             "actor_login": r.split("/")[0], "repo_name": r, "org_login": None,
                             "action": None, "ref_type": "repository"})
        recs.sort(key=lambda x: x["created_at"])
        name = f"sample_{hour_start:%Y-%m-%d_%H}.json"
        write_jsonl_atomically(recs, name, target, STAGING_DIR)
        total += len(recs)
    print(f"Wrote {total:,} synthetic events ({a.hours} hourly files) to {target}")


if __name__ == "__main__":
    main()
