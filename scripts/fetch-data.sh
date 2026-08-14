#!/usr/bin/env bash
#
# Downloads hours of the public GH Archive event stream into data/raw/.
#
#   ./scripts/fetch-data.sh 2024-01-15           # one hour  (00:00 UTC)
#   ./scripts/fetch-data.sh 2024-01-15 15        # one hour  (15:00 UTC)
#   ./scripts/fetch-data.sh 2024-01-15 0 23      # a whole day, about 3 GB compressed
#
# The files are gzipped newline-delimited JSON and both pipelines read them as they are.

set -euo pipefail

DATE=${1:-}
FROM=${2:-0}
TO=${3:-$FROM}

if [[ -z "$DATE" ]]; then
  sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="$ROOT/data/raw"
mkdir -p "$DEST"

for (( hour = FROM; hour <= TO; hour++ )); do
  name="$DATE-$hour.json.gz"
  target="$DEST/$name"

  if [[ -s "$target" ]]; then
    echo "have    $name"
    continue
  fi

  echo "fetch   $name"
  # --fail so a 404 for an hour GH Archive never published leaves no truncated file behind.
  if ! curl --fail --location --silent --show-error --output "$target" \
      "https://data.gharchive.org/$name"; then
    rm -f "$target"
    echo "missing $name (skipped)" >&2
  fi
done

echo
echo "Downloaded into data/raw/. Run the pipelines against it with:"
echo "  sbt 'spark/runMain ghpulse.spark.SparkPipeline --input data/raw'"
echo "  sbt 'flink/runMain ghpulse.flink.FlinkPipeline --input data/raw'"
