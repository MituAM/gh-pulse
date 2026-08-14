#!/usr/bin/env python3
"""Rebuilds data/sample/events.jsonl from a full GH Archive hour on stdin.

    curl -sS https://data.gharchive.org/2024-01-15-15.json.gz \
      | gzip -cd | python3 scripts/make-sample.py > data/sample/events.jsonl

Keeps every EVERY_NTH record so the sample spans the whole hour with the hour's own mix of event
types, and projects each payload down to the fields the pipelines read. Field names and nesting
are unchanged, so unmodified GH Archive files decode through the same path.
"""
import json
import re
import sys

EVERY_NTH = 200

# GitHub's corpus contains commit messages with unpaired UTF-16 surrogates, which are not
# encodable as UTF-8. Replace them so the fixture is a valid UTF-8 file.
SURROGATE = re.compile("[\ud800-\udfff]")


def clean(value):
    if isinstance(value, str):
        return SURROGATE.sub("�", value)
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean(v) for v in value]
    return value


def project(event):
    kind = event["type"]
    source = event.get("payload") or {}
    payload = {}

    if "action" in source:
        payload["action"] = source["action"]

    if kind == "PushEvent":
        payload["push_id"] = source.get("push_id")
        payload["size"] = source.get("size")
        payload["distinct_size"] = source.get("distinct_size")
        payload["ref"] = source.get("ref")
        payload["commits"] = [
            {
                "sha": c.get("sha"),
                "author": {
                    "name": (c.get("author") or {}).get("name"),
                    "email": (c.get("author") or {}).get("email"),
                },
                "message": c.get("message"),
                "distinct": c.get("distinct"),
            }
            for c in (source.get("commits") or [])
        ]
    elif kind == "PullRequestEvent":
        pull_request = source.get("pull_request") or {}
        payload["number"] = source.get("number")
        payload["pull_request"] = {
            "merged": pull_request.get("merged"),
            "additions": pull_request.get("additions"),
            "deletions": pull_request.get("deletions"),
            "changed_files": pull_request.get("changed_files"),
            "draft": pull_request.get("draft"),
        }
    elif kind in ("CreateEvent", "DeleteEvent"):
        payload["ref_type"] = source.get("ref_type")
        payload["ref"] = source.get("ref")

    projected = {
        "id": event["id"],
        "type": kind,
        "actor": {"id": event["actor"]["id"], "login": event["actor"]["login"]},
        "repo": {"id": event["repo"]["id"], "name": event["repo"]["name"]},
        "public": event.get("public", True),
        "created_at": event["created_at"],
        "payload": payload,
    }
    if event.get("org"):
        projected["org"] = {"id": event["org"]["id"], "login": event["org"]["login"]}
    return clean(projected)


def main():
    for index, line in enumerate(sys.stdin):
        if index % EVERY_NTH:
            continue
        print(json.dumps(project(json.loads(line)), separators=(",", ":"), ensure_ascii=False))


if __name__ == "__main__":
    main()
