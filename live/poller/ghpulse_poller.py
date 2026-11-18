#!/usr/bin/env python3
"""gh-pulse poller: GitHub's public Events API -> newline-delimited JSON batch files.

One file per poll is dropped into --out, written under a hidden name and renamed into place, so
the Flink job (which follows the directory and skips names starting with '.') never reads a
half-written batch. Each line is one event in exactly the envelope GH Archive uses, trimmed to the
fields ghpulse.core.codec.GitHubArchiveDecoder reads.

What this feed is, honestly:
  * a sample: GET /events returns at most 300 events per poll, and GitHub asks for at most one
    poll per X-Poll-Interval (60 s). The firehose is several thousand events a minute.
  * delayed: events show up at least ~5 minutes after they happen, with a tail of much older ones
    (hours). Anything older than --max-age is dropped here rather than polluting "live" windows.

Standard library only; runs on any Debian python3.

    GITHUB_TOKEN=github_pat_... python3 ghpulse_poller.py --out /var/lib/gh-pulse/incoming \
        --status /var/lib/gh-pulse/public/ingest.json
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import os
import re
import signal
import statistics
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

API = "https://api.github.com/events"
USER_AGENT = "gh-pulse-poller (+https://github.com/MituAM/gh-pulse)"
LINK_NEXT = re.compile(r'<([^>]+)>;\s*rel="next"')

log = logging.getLogger("gh-pulse-poller")


def slim(event: dict) -> dict:
    """Keeps only what the decoder reads. Issue and PR bodies make raw events ~10x larger."""
    out = {
        "id": event["id"],
        "type": event["type"],
        "created_at": event["created_at"],
        "actor": {"id": event["actor"]["id"], "login": event["actor"]["login"]},
        "repo": {"id": event["repo"]["id"], "name": event["repo"]["name"]},
    }
    if event.get("org"):
        out["org"] = {"login": event["org"].get("login")}
    payload = event.get("payload") or {}
    kept = {k: payload[k] for k in ("action", "ref", "ref_type", "size", "distinct_size", "number")
            if k in payload}
    if isinstance(payload.get("commits"), list):
        # Only the pre-2025 format has commits. Author names, emails and messages are dropped (not
        # used, and personal data); the empty "author" object keeps the decoder's cursor happy.
        kept["commits"] = [
            {"sha": c.get("sha", ""), "author": {}, "distinct": c.get("distinct", True)}
            for c in payload["commits"]
        ]
    pr = payload.get("pull_request")
    if isinstance(pr, dict):
        kept["pull_request"] = {k: pr[k] for k in ("merged", "additions", "deletions",
                                                   "changed_files") if k in pr}
    out["payload"] = kept
    return out


def parse_time(raw: str) -> float:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()


class Poller:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.out = Path(args.out)
        self.out.mkdir(parents=True, exist_ok=True)
        self.token = os.environ.get(args.token_env, "").strip() or None
        # Unauthenticated clients get 60 requests an hour: one page a minute is already all of it.
        self.pages = args.pages if self.token else 1
        self.etag: str | None = None
        self.seen: collections.OrderedDict[str, None] = collections.OrderedDict()
        self.total = 0
        self.running = True
        self.status: dict = {"authenticated": bool(self.token), "eventsTotal": 0}

    # -- HTTP -------------------------------------------------------------------------------

    def get(self, url: str, etag: str | None = None):
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": USER_AGENT,
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if etag:
            headers["If-None-Match"] = etag
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, response.headers, response.read()
        except urllib.error.HTTPError as e:  # 304 and 4xx/5xx arrive here, with headers
            return e.code, e.headers, e.read()

    # -- one poll ---------------------------------------------------------------------------

    def poll(self) -> float:
        """Polls once and returns how many seconds to wait before the next poll."""
        url = f"{API}?per_page={self.args.per_page}"
        status, headers, body = self.get(url, self.etag)
        interval = max(float(headers.get("X-Poll-Interval", 60)), self.args.min_interval)
        self.status["rateLimitRemaining"] = headers.get("X-RateLimit-Remaining")
        self.status["pollIntervalSeconds"] = interval

        if status == 304:
            log.debug("304 not modified")
            return interval
        if status in (403, 429):
            return self.rate_limited(headers, interval)
        if status != 200:
            raise RuntimeError(f"GET /events -> HTTP {status}: {body[:200]!r}")

        self.etag = headers.get("ETag")
        events = json.loads(body)
        next_url = self.next_link(headers)
        for _ in range(self.pages - 1):
            if not next_url or not self.running:
                break
            status, headers, body = self.get(next_url)
            if status != 200:
                log.warning("page fetch stopped early: HTTP %s", status)
                break
            events.extend(json.loads(body))
            next_url = self.next_link(headers)

        self.ingest(events)
        return interval

    @staticmethod
    def next_link(headers) -> str | None:
        match = LINK_NEXT.search(headers.get("Link", "") or "")
        return match.group(1) if match else None

    def rate_limited(self, headers, interval: float) -> float:
        retry_after = headers.get("Retry-After")
        reset = headers.get("X-RateLimit-Reset")
        if retry_after:
            wait = float(retry_after)
        elif reset:
            wait = max(float(reset) - time.time(), interval)
        else:
            wait = max(interval, 120.0)
        log.warning("rate limited; sleeping %.0fs (authenticated=%s)", wait, bool(self.token))
        return wait + 1

    # -- output -----------------------------------------------------------------------------

    def ingest(self, events: list[dict]) -> None:
        now = time.time()
        fresh, duplicates, stale, lags = [], 0, 0, []
        for event in events:
            event_id = event.get("id")
            if not event_id or event_id in self.seen:
                duplicates += 1
                continue
            self.remember(event_id)
            lag = now - parse_time(event["created_at"])
            if lag > self.args.max_age_minutes * 60:
                stale += 1
                continue
            lags.append(lag)
            fresh.append(slim(event))

        if fresh:
            self.write_batch(fresh)
        self.total += len(fresh)
        median_lag = statistics.median(lags) if lags else None
        log.info("poll: %d fetched, %d new, %d duplicate, %d older than %d min, median lag %s",
                 len(events), len(fresh), duplicates, stale, self.args.max_age_minutes,
                 f"{median_lag / 60:.1f} min" if median_lag is not None else "-")
        self.status.update(
            lastPollAt=int(now * 1000),
            eventsLastPoll=len(fresh),
            eventsTotal=self.total,
            staleDroppedLastPoll=stale,
            medianLagSeconds=round(median_lag) if median_lag is not None else None,
        )

    def remember(self, event_id: str) -> None:
        self.seen[event_id] = None
        while len(self.seen) > 20_000:
            self.seen.popitem(last=False)

    def write_batch(self, events: list[dict]) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        final = self.out / f"batch-{stamp}.jsonl"
        tmp = self.out / f".batch-{stamp}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
                f.write("\n")
        os.replace(tmp, final)

    def cleanup(self) -> None:
        """Bounds the directory, which is also what the Flink job replays after a restart."""
        horizon = time.time() - self.args.retain_minutes * 60
        for path in self.out.glob("batch-*.jsonl"):
            try:
                if path.stat().st_mtime < horizon:
                    path.unlink()
            except FileNotFoundError:
                pass

    def write_status(self) -> None:
        if not self.args.status:
            return
        target = Path(self.args.status)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.tmp")
        tmp.write_text(json.dumps(self.status), encoding="utf-8")
        os.replace(tmp, target)

    # -- loop -------------------------------------------------------------------------------

    def run(self) -> None:
        log.info("polling %s: %d page(s) per poll, authenticated=%s, writing to %s",
                 API, self.pages, bool(self.token), self.out)
        backoff = 5.0
        while self.running:
            try:
                wait = self.poll()
                backoff = 5.0
                self.status["lastError"] = None
            except Exception as e:  # network blips, 5xx, bad JSON: back off, never die
                log.warning("poll failed: %s; retrying in %.0fs", e, backoff)
                self.status["lastError"] = str(e)[:200]
                wait, backoff = backoff, min(backoff * 2, 300.0)
            self.cleanup()
            self.write_status()
            if self.args.once:
                break
            self.sleep(wait)

    def sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while self.running and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))

    def stop(self, *_):
        log.info("stopping")
        self.running = False


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--out", required=True, help="directory the Flink job follows")
    p.add_argument("--status", help="small JSON health file for the dashboard (optional)")
    p.add_argument("--pages", type=int, default=3, help="pages per poll with a token (max 3)")
    p.add_argument("--per-page", type=int, default=100, help="events per page (max 100)")
    p.add_argument("--max-age-minutes", type=int, default=30, help="drop events older than this")
    p.add_argument("--retain-minutes", type=int, default=120, help="delete batch files after")
    p.add_argument("--min-interval", type=float, default=0.0,
                   help="never poll more often than this; X-Poll-Interval always applies too")
    p.add_argument("--token-env", default="GITHUB_TOKEN", help="env var holding a GitHub token")
    p.add_argument("--once", action="store_true", help="poll once and exit")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()
    args.pages = max(1, min(args.pages, 3))
    args.per_page = max(1, min(args.per_page, 100))

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(message)s")
    poller = Poller(args)
    signal.signal(signal.SIGTERM, poller.stop)
    signal.signal(signal.SIGINT, poller.stop)
    poller.run()


if __name__ == "__main__":
    main()
