#!/usr/bin/env bash
# Installs or upgrades the gh-pulse live dashboard on Debian 12/13. Idempotent: run it again after
# copying a new jar or new web files and it redeploys and restarts what changed.
#
#   sudo ./live/deploy/install.sh            # from the copied live/ directory, jar inside it
#
# Expects, relative to this script:  ../gh-pulse-live.jar  ../poller/  ../web/  ./systemd/  ./nginx/
set -euo pipefail

if [[ $EUID -ne 0 ]]; then echo "run as root (sudo)" >&2; exit 1; fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIVE="$(dirname "$HERE")"
JAR="${JAR:-$LIVE/gh-pulse-live.jar}"
[[ -f "$JAR" ]] || { echo "missing $JAR — copy modules/live/target/scala-2.12/gh-pulse-live.jar there" >&2; exit 1; }

echo "==> packages"
JRE=openjdk-21-jre-headless
apt-cache show "$JRE" >/dev/null 2>&1 || JRE=openjdk-17-jre-headless
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 nginx curl "$JRE" >/dev/null

echo "==> user and directories"
id ghpulse >/dev/null 2>&1 ||
  useradd --system --home-dir /var/lib/gh-pulse --no-create-home --shell /usr/sbin/nologin ghpulse
install -d -o root -g root -m 0755 /opt/gh-pulse /opt/gh-pulse/poller /opt/gh-pulse/web
install -d -o ghpulse -g ghpulse -m 0755 /var/lib/gh-pulse /var/lib/gh-pulse/incoming /var/lib/gh-pulse/public
install -d -o root -g root -m 0755 /etc/gh-pulse

echo "==> application files"
install -m 0644 "$JAR" /opt/gh-pulse/gh-pulse-live.jar
install -m 0755 "$LIVE/poller/ghpulse_poller.py" /opt/gh-pulse/poller/ghpulse_poller.py
install -m 0644 "$LIVE"/web/* /opt/gh-pulse/web/

if [[ -f "$HERE/poller.env" ]]; then
  # Shipped alongside this script (gitignored): install it root-only, then the copy can go.
  install -o root -g root -m 0600 "$HERE/poller.env" /etc/gh-pulse/poller.env
  echo "    installed the GitHub token to /etc/gh-pulse/poller.env"
  echo "    you can now delete the copy: rm $HERE/poller.env"
elif [[ ! -f /etc/gh-pulse/poller.env ]]; then
  printf 'GITHUB_TOKEN=\n' > /etc/gh-pulse/poller.env
  chmod 0600 /etc/gh-pulse/poller.env
  echo "    created /etc/gh-pulse/poller.env — put a GitHub token in it (strongly recommended)"
fi

echo "==> nginx (127.0.0.1:8080)"
install -m 0644 "$HERE/nginx/gh-pulse.conf" /etc/nginx/sites-available/gh-pulse.conf
ln -sf /etc/nginx/sites-available/gh-pulse.conf /etc/nginx/sites-enabled/gh-pulse.conf
nginx -t
systemctl enable --now nginx >/dev/null
systemctl reload nginx

echo "==> systemd units"
install -m 0644 "$HERE"/systemd/gh-pulse-*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable gh-pulse-poller.service gh-pulse-flink.service >/dev/null
systemctl restart gh-pulse-poller.service gh-pulse-flink.service

echo
systemctl --no-pager --lines=0 status gh-pulse-poller gh-pulse-flink || true
cat <<'EOF'

Installed. Check it:
  journalctl -u gh-pulse-poller -f          # one line per poll
  journalctl -u gh-pulse-flink -f           # quiet unless something is wrong
  curl -s localhost:8080/api/ingest.json    # poller health, immediately
  curl -s localhost:8080/api/state.json     # dashboard data, after the first window closes

Then point a Cloudflare Tunnel at http://127.0.0.1:8080 (live/README.md, step 4).
EOF
