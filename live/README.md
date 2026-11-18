# gh-pulse live

A public, real-time dashboard on top of gh-pulse: **who is spamming GitHub right now** (burst
detection) and **which repositories are blowing up** (the weighted trending leaderboard), computed
continuously by the same Flink operators and shared Scala rules as the batch/stream parity suite.

Running at **[radar.algotricks.com](https://radar.algotricks.com)**. The commands below use
`pulse.example.com` as a placeholder for your own hostname.

```
 GitHub Events API ──poll (ETag, X-Poll-Interval)──▶ poller (python3, stdlib)
                                                        │  one NDJSON file per poll,
                                                        ▼  renamed into place
                                        /var/lib/gh-pulse/incoming/
                                                        │  FileSource.monitorContinuously
                                                        ▼
                           LiveDashboardJob (java -jar, embedded Flink)
                           ├─ LiveBursts      BurstDetector.step + target repos
                           └─ TrendingRepos   sliding windows, two-stage top-15
                                                        │  debounced, atomic rename
                                                        ▼
                                  /var/lib/gh-pulse/public/state.json
                                                        │
                     nginx 127.0.0.1:8080 (static files + /api/*.json, s-maxage=5)
                                                        │
                       cloudflared ──tunnel──▶ Cloudflare edge cache ──▶ visitors
```

There is no application server: the page is static, the data is two JSON files, and Cloudflare
caches both. However many visitors arrive, the home server sees roughly one request per file every
few seconds.

## What the data is (and is not)

Measured against the live API while building this (2026-11):

| | |
|---|---|
| **Sample, not firehose** | `GET /events` returns at most 300 events per poll, and GitHub asks for one poll per `X-Poll-Interval` (60 s). The firehose is several thousand events a minute. |
| **Delayed** | Nothing arrives sooner than ~5 min after it happened; the median is ~7 min, with a tail hours old. The poller drops anything older than 30 min. |
| **Trimmed payloads** | Since 2025-10-07 `PushEvent` has no commits or counts, and `PullRequestEvent` has no `merged`/`additions`/`deletions` (a merge is now `action: "merged"`). This applies to GH Archive too, because it mirrors this API. `ActivityStats.of` handles both formats. |

Because of the sampling, the live job's default burst rule is **5 events in 10 minutes**, not the
batch job's 10-in-5: five *sampled* events stand for dozens of real ones. Both the rule and the
dashboard label come from the same flags, so they never disagree.

## 1 · Try it locally

```bash
sbt live/assembly          # → modules/live/target/scala-2.12/gh-pulse-live.jar (Flink included)

# a) Instant preview from the committed 2024 sample
java -jar modules/live/target/scala-2.12/gh-pulse-live.jar --once \
  --input data/sample/events.jsonl --state-file live/web/api/state.json \
  --window 10 --burst-events 10 --burst-window 5
python3 -m http.server 8080 -d live/web      # open http://localhost:8080

# b) The real live feed (two terminals)
GITHUB_TOKEN=github_pat_... python3 live/poller/ghpulse_poller.py \
  --out tmp/incoming --status live/web/api/ingest.json
java -jar modules/live/target/scala-2.12/gh-pulse-live.jar \
  --input tmp/incoming --state-file live/web/api/state.json
```

The first bursts appear within minutes. The leaderboard appears once the first window closes,
about 10 minutes after start because of the feed delay.

## 2 · A GitHub token

Without a token the API allows 60 requests an hour, which is one page per minute. With one it
allows 5,000, and the poller reads all three pages each minute. That triples the sample and still
uses under 4% of the quota.

GitHub → Settings → Developer settings → **Fine-grained personal access tokens** → Generate:
*Repository access: Public repositories (read-only)*, **no permissions**, expiry ≤ 1 year (set a
reminder to rotate it). The token can read nothing that is not already public.

## 3 · Deploy on the Debian server

On your PC, put the token in `live/deploy/poller.env` (gitignored) as `GITHUB_TOKEN=github_pat_...`, then:

```bash
sbt live/assembly
cp modules/live/target/scala-2.12/gh-pulse-live.jar live/
scp -r live you@server:~/gh-pulse-live          # PowerShell's scp works too
```

On the server:

```bash
sudo ~/gh-pulse-live/deploy/install.sh          # installs the token to /etc/gh-pulse (root, 0600)
rm ~/gh-pulse-live/deploy/poller.env            # the copy is no longer needed

journalctl -u gh-pulse-poller -f                # "poll: 300 fetched, 212 new, ... median lag 6.8 min"
curl -s localhost:8080/api/state.json | head -c 400
```

`install.sh` is idempotent. It installs `python3`, `nginx` and a headless JRE (21, or 17 on
Debian 12), creates a `ghpulse` system user, puts the code in `/opt/gh-pulse` and the data in
`/var/lib/gh-pulse`, then enables three things:

| unit | what | restarts |
|---|---|---|
| `gh-pulse-poller.service` | polls the API, writes batches + `ingest.json` | always, 10 s |
| `gh-pulse-flink.service` | the Flink job, writes `state.json` | always, 15 s; replays the last 2 h of batches to rebuild its state |
| `nginx` (`sites-enabled/gh-pulse.conf`) | static site on **127.0.0.1:8080 only** | Debian default |

Both gh-pulse units run sandboxed (`ProtectSystem=strict`, write access only to
`/var/lib/gh-pulse`) and memory-capped. To upgrade, copy the new files and run `install.sh` again.
To tune the job, put flags in `/etc/gh-pulse/flink.env`, e.g.
`GH_PULSE_ARGS=--burst-events 8 --slide 1`, then restart `gh-pulse-flink`.

Expected footprint: ~600–900 MB RAM for the JVM, ~30 MB for the poller, under 10 MB of disk,
and near-zero CPU.

## 4 · Cloudflare Tunnel

No ports are opened on the router; `cloudflared` dials out to Cloudflare.

```bash
# install (Cloudflare's apt repository)
sudo mkdir -p --mode=0755 /usr/share/keyrings
curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg | sudo tee /usr/share/keyrings/cloudflare-main.gpg >/dev/null
echo 'deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main' \
  | sudo tee /etc/apt/sources.list.d/cloudflared.list
sudo apt-get update && sudo apt-get install -y cloudflared

# create the tunnel (login prints a URL — open it on any machine and pick your domain)
cloudflared tunnel login
cloudflared tunnel create gh-pulse                 # prints the tunnel UUID
cloudflared tunnel route dns gh-pulse pulse.example.com

# run it as a service
sudo mkdir -p /etc/cloudflared
sudo cp ~/.cloudflared/<TUNNEL-UUID>.json /etc/cloudflared/
sudo cp ~/gh-pulse-live/deploy/cloudflared/config.yml /etc/cloudflared/config.yml
sudo nano /etc/cloudflared/config.yml              # fill in the UUID (twice) and your hostname
sudo cloudflared tunnel ingress validate           # /etc/cloudflared is on the default search path
sudo cloudflared service install
sudo systemctl enable --now cloudflared
```

(Alternative: create the tunnel in the Zero Trust dashboard under Networks → Tunnels, add a public
hostname pointing to `http://127.0.0.1:8080`, then run the `sudo cloudflared service install <token>`
command it shows. That needs no config file.)

## 5 · Cloudflare edge settings for traffic spikes

nginx already sends the right headers: `s-maxage=5` on the JSON, 5 min to 1 h on assets, and
`stale-while-revalidate`/`stale-if-error`. By default Cloudflare does **not** cache JSON or HTML,
so tell it to:

1. **Caching → Cache Rules → Create rule**: *Hostname equals `pulse.example.com`* → **Eligible for
   cache**; Edge TTL **Use cache-control header if present**; Browser TTL **Respect origin**.
2. **Caching → Tiered Cache → Smart Tiered Caching**: on. Edge locations then fill from one upper
   tier instead of each asking your server.
3. **SSL/TLS → Edge Certificates**: Always Use HTTPS on, minimum TLS 1.2.
4. **Security → WAF → Rate limiting rules** (one is free): URI path starts with `/api/`, more than
   50 requests per 10 s per IP → block for 10 s. A real browser makes 4.
5. Leave **Rocket Loader** off; the page's CSP would block it anyway. Use *Under Attack* mode
   only if you are actually attacked, because its challenge interrupts the page's JSON polling.

Check it from outside:

```bash
curl -sI https://pulse.example.com/api/state.json | grep -iE 'cf-cache-status|cache-control|age'
# cf-cache-status: HIT  (MISS/EXPIRED once every 5 s is expected)
```

If the server goes down, visitors keep getting the last snapshot from Cloudflare for up to 10
minutes (`stale-if-error`), and the page marks itself **STALE** instead of breaking.
