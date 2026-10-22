"""Small helpers shared by the data-capture scripts (no Spark needed)."""
import json
import os


def slim_event(ev):
    """Keep only the fields the project uses. This shrinks the data ~20x."""
    payload = ev.get("payload") or {}
    return {
        "id": ev.get("id"),
        "type": ev.get("type"),
        "created_at": ev.get("created_at"),
        "actor_login": (ev.get("actor") or {}).get("login"),
        "repo_name": (ev.get("repo") or {}).get("name"),
        "org_login": (ev.get("org") or {}).get("login"),
        "action": payload.get("action"),      # opened / closed / started ...
        "ref_type": payload.get("ref_type"),  # repository / branch / tag (CreateEvent)
    }


def write_jsonl_atomically(records, filename, target_dir, staging_dir):
    """
    Flume's spooling-directory source must never see a half-written file,
    so we write to a staging folder first and then move the finished file in.
    """
    os.makedirs(target_dir, exist_ok=True)
    os.makedirs(staging_dir, exist_ok=True)
    tmp = os.path.join(staging_dir, filename)
    with open(tmp, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    os.replace(tmp, os.path.join(target_dir, filename))


def github_headers(token):
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "github-trend-radar-student-project",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers
