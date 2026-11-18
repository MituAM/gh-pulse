# gh-pulse

[![CI](https://github.com/MituAM/gh-pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/MituAM/gh-pulse/actions/workflows/ci.yml)
[![Scala 2.12](https://img.shields.io/badge/scala-2.12-red.svg)](https://www.scala-lang.org/)
[![Spark 3.5](https://img.shields.io/badge/spark-3.5.1-e25a1c.svg)](https://spark.apache.org/)
[![Flink 1.18](https://img.shields.io/badge/flink-1.18.1-e6526f.svg)](https://flink.apache.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Live dashboard](https://img.shields.io/badge/live-radar.algotricks.com-39ff88.svg)](https://radar.algotricks.com)

Analytics over the public GitHub event firehose, computed twice: once as a Spark batch job and once as a Flink streaming job, from **one shared Scala core that neither engine can bypass**.

The interesting part is not that both engines are used. It is that the business rules — what counts as a commit, how a repository is scored, what makes a burst — exist exactly once, as pure Scala with no dependency on either runtime, and both engines are held to the same reference answers by the test suite. A rule change lands in batch and in streaming simultaneously, or the build goes red.

**See it running: [radar.algotricks.com](https://radar.algotricks.com)** — the same Flink operators,
fed live from GitHub, showing which accounts are bursting and which repositories are blowing up
right now.

```
                    ┌───────────────────────────────────────────────┐
                    │  core   pure Scala · no engine · no I/O       │
                    │                                               │
                    │  GitHubEvent  EventKind ADT   EventDecoder    │
                    │  ActivityStats + Monoid       ScoreWeights    │
                    │  TopK (bounded, mergeable)    BurstDetector   │
                    └───────────────┬───────────────┬───────────────┘
                                    │               │
                  ┌─────────────────┘               └──────────────────┐
                  ▼                                                    ▼
    ┌─────────────────────────────┐                    ┌──────────────────────────────┐
    │  spark   batch              │                    │  flink   streaming           │
    │                             │                    │                              │
    │  Dataset + RDD leaderboard  │                    │  event-time windows          │
    │  SQL window functions       │                    │  two-stage top-N w/ timers   │
    │  self-join collaboration    │                    │  keyed state burst detection │
    │  aggregateByKey · treeAgg   │                    │  watermarks · metrics        │
    └─────────────────────────────┘                    └──────────────────────────────┘
                  └─────────────────┐               ┌──────────────────┘
                                    ▼               ▼
                        ┌───────────────────────────────────────┐
                        │  Reference — the answers in plain     │
                        │  Scala. Both engines assert against   │
                        │  it, so batch == streaming is tested. │
                        └───────────────────────────────────────┘
```

## Quick start

Needs only a JDK (11 or 17) and `sbt`. Nothing to install, no cluster, no network — a sample of a real GH Archive hour is committed.

```bash
sbt test        # 121 tests: unit, property-based, Spark and Flink integration
sbt demoSpark   # batch analytics over the sample
sbt demoFlink   # the same analytics as a stream, on an embedded Flink cluster
```

### `sbt demoSpark`

```
gh-pulse (batch)
  input     data/sample/events.jsonl
  decoder   gharchive
  events    1335 accepted, 0 rejected

Top 10 repositories
  #  repository                                    score  commits   prs  stars
  1  LMAO-armv8/kernel_samsung_r0q                  51.0       51     0      0
  2  dlangille/sanoid                               36.0       36     0      0
  3  erickwill/gpt4all-ui                           24.0       24     0      0

Busiest repositories per window
+-----------------+--------------+-------------------------------+-----+-----------+-------------+
|window_label     |rank_in_window|repo                           |score|score_delta|rolling_score|
+-----------------+--------------+-------------------------------+-----+-----------+-------------+
|2024-01-15T15:40Z|1             |LMAO-armv8/kernel_samsung_r0q  |32.0 |28.0       |39.0         |
|2024-01-15T15:50Z|2             |LMAO-armv8/kernel_samsung_r0q  |7.0  |-25.0      |43.0         |
+-----------------+--------------+-------------------------------+-----+-----------+-------------+

Actor bursts (10+ events in 5 minutes)
actor                      events  from                 to
github-actions[bot]            10  2024-01-15T15:04Z    2024-01-15T15:08Z
ion561sdag                     12  2024-01-15T15:34Z    2024-01-15T15:38Z
inse2233tto                    13  2024-01-15T15:53Z    2024-01-15T15:58Z
```

### `sbt demoFlink`

```
trending:12> 2024-01-15T15:01Z..2024-01-15T15:11Z #1  dlangille/sanoid              36.0
trending:7>  2024-01-15T15:06Z..2024-01-15T15:16Z #1  dlangille/sanoid              36.0
trending:3>  2024-01-15T15:25Z..2024-01-15T15:35Z #1  itsmacr8/first-contributions   8.0
bursts:4>    github-actions[bot]        10 events  2024-01-15T15:04Z -> 2024-01-15T15:08Z
bursts:5>    ion561sdag                 12 events  2024-01-15T15:34Z -> 2024-01-15T15:38Z
```

The same bursts, from the same bytes, found by two completely different execution models.

### Running on the real firehose

[GH Archive](https://www.gharchive.org/) publishes every public GitHub event, one gzipped file per
hour, going back to 2011.

```bash
./scripts/fetch-data.sh 2024-01-15 0 23      # a day, ~3 GB compressed
sbt 'spark/runMain ghpulse.spark.SparkPipeline --input data/raw --top 25'
```

Both jobs read files, directories and globs, and decompress `.gz` on the way in.
Windows users: `scripts/fetch-data.ps1` does the same thing.

## Live dashboard

**[radar.algotricks.com](https://radar.algotricks.com)** runs the streaming half of this project
around the clock, on a home server behind a Cloudflare Tunnel:

```
GitHub Events API → poller (Python) → NDJSON files → LiveDashboardJob (Flink, java -jar)
                                                      ├─ bursts:   BurstDetector + target repos
                                                      └─ trending: sliding 60-min top 15
                  → state.json → nginx → Cloudflare cache → browsers
```

The burst rule and the scoring are the shared core, unchanged; the live job only adds a continuous
file source, repository attribution for bursts, and a writer that turns both result streams into
one JSON snapshot. There is no application server — the page is static and Cloudflare caches the
data, so a traffic spike costs the home server one request every few seconds.

The public Events API is a *sample* of the firehose, delayed by about five minutes, so the live job
uses a burst rule tuned for that (5 events in 10 minutes). [`live/README.md`](live/README.md)
explains the data's limits and how to run the whole stack locally or deploy it.

## Using your own data

Three levels, depending on how far your data is from GitHub's.

**1. Same shape, different file.** If your records look like GH Archive's, point `--input`
anywhere Spark or Flink can read from — a path, a directory, a glob, HDFS, S3:

```bash
sbt 'spark/runMain ghpulse.spark.SparkPipeline --input s3a://my-bucket/events/*.json.gz'
```

**2. Different shape.** Implement one trait and name it on the command line. Nothing else changes —
every analysis is written against the domain model, not the wire format:

```scala
package com.example

import ghpulse.core.codec.EventDecoder
import ghpulse.core.domain._

object AuditLogDecoder extends EventDecoder {
  def decode(record: String): Either[String, GitHubEvent] = record.split(",", -1) match {
    case Array(id, at, user, project) =>
      Right(GitHubEvent(id, "PushEvent", at.toLong, Actor(0L, user), Repo(0L, project),
                        None, Payload.empty.copy(distinctSize = Some(1))))
    case columns => Left(s"expected 4 columns, got ${columns.length}")
  }
}
```

```bash
sbt 'spark/runMain ghpulse.spark.SparkPipeline --input audit.csv --decoder com.example.AuditLogDecoder'
```

Records your decoder rejects are counted, not fatal — a Spark accumulator and a Flink metric report
them, and the job finishes. A decoder that *throws* is contained the same way, because a job should
not die over one bad line. There is a worked example in
[`EventDecoderSpec.scala`](modules/core/src/test/scala/ghpulse/core/codec/EventDecoderSpec.scala).

**3. Different questions.** The analyses in `core/analytics` are pure functions over
`List[GitHubEvent]`. Add one there with a unit test, then wire it into either engine — the Flink
operator for burst detection is 40 lines because all it does is decide *when* the shared rule sees
each event.

## What it computes

| Analysis | Batch (Spark) | Streaming (Flink) |
|---|---|---|
| **Repository leaderboard** — weighted activity score, top *k* | `Dataset` typed aggregation and an `RDD` version with `aggregateByKey` + `treeAggregate` | two-stage: keyed window aggregate, then re-keyed by window end and ranked on an event-time timer |
| **Activity timeline** — per-window score, rolling total, period-over-period change, rank | Spark SQL window functions over the typed aggregates | tumbling and sliding event-time windows |
| **Actor bursts** — *n* events from one person inside *m* minutes | shuffle per actor, sort, replay the shared rule | keyed `ValueState` + event-time timers, replaying the same rule in the same order |
| **Collaboration graph** — people sharing repositories | self-join with a `<` predicate to drop self-pairs and mirrors | — |

Scoring weights are configuration, not a hard-coded formula: `--weight-star 0` and stars stop
counting, in both engines at once.

## Design notes

**Why the domain model is a flat product.** `GitHubEvent` uses only primitives, `Option`, `List`
and case classes. That is exactly what Spark can derive an `Encoder` for and Flink a
`CaseClassSerializer` for, so one type crosses both engines with no custom serialization. The
sealed `EventKind` hierarchy — which analytics pattern match on exhaustively — is a *method*, not a
field, because a sealed trait would defeat both derivations.

**Why `ActivityStats` is a monoid.** Spark's `reduceByKey` re-associates freely across partitions;
Flink's window aggregation folds one record at a time. Both are only correct if combine is
associative with an identity, so [`MonoidLawsSpec`](modules/core/src/test/scala/ghpulse/core/analytics/MonoidLawsSpec.scala)
property-checks those laws rather than assuming them. The same instance backs `reduceGroups`,
`aggregateByKey` and Flink's `AggregateFunction.merge`.

**Why `TopK` needs a total order.** Merging bounded top-*k* results is associative only if no two
distinct elements compare equal — otherwise which of a tied pair survives depends on the merge
order. Ranking therefore breaks ties on repository name, and the property test generates elements
where the ordering really is total.

**Why the Flink burst operator buffers.** `processElement` sees records in arrival order, which for
an out-of-order stream is not the order they happened. Since the rule's answer depends on that
order, events are held against an event-time timer and replayed once the watermark has passed them.
That is what turns "batch and streaming roughly agree" into an equality assertion.

**Why one decoder, not two.** Spark reads text and applies the shared `EventDecoder` rather than
using `spark.read.json`. Spark's own reader is faster and splittable; a second parser would also be
a second set of bugs, and the parity claim would be worth much less. The trade-off is deliberate,
and swapping it back is a one-line change in [`EventSource`](modules/spark/src/main/scala/ghpulse/spark/EventSource.scala).

**Why a push with no commit count scores one commit.** Since 2025-10-07 GitHub's Events API — and
therefore GH Archive, which mirrors it — sends `PushEvent` without commits or counts, and
`PullRequestEvent` without diff stats, reporting a merge as `action: "merged"`. The decoder never
failed on the new shape; the scores silently went to zero. `ActivityStats.of` now reads both
shapes, and tests pin each one.

**Why Scala 2.12.** Spark 3.5 and Flink 1.18 both publish their Scala APIs for 2.12 only. Flink
deprecated its Scala DataStream API in 1.18 (FLIP-265); it remains the most idiomatic way to drive
Flink from Scala, and the deprecation warnings are muted in that module alone.

## Testing

```
sbt test          # everything
sbt core/test     # fast: pure logic, no engine starts
sbt verify        # scalafmt check + the full suite, which is what CI runs
```

121 tests across three layers:

- **Property-based** (ScalaCheck) — monoid laws, `TopK` against a full sort, and the burst rule
  against a direct statement of what a burst is.
- **Reference-based** — [`Reference`](modules/core/src/test/scala/ghpulse/core/testkit/Reference.scala)
  computes every answer in single-threaded Scala. Spark asserts `batch == Reference`; Flink asserts
  `streaming == Reference`. Neither engine is ever compared to the other directly — equality with a
  common third value is what makes them equal, and it identifies which one is wrong when they are
  not.
- **Integration** — Spark suites run a real local `SparkSession` over data spread across several
  partitions (the leaderboard is asserted identical at 1, 2, 5 and 11 partitions);
  [`BatchStreamParitySpec`](modules/flink/src/test/scala/ghpulse/flink/BatchStreamParitySpec.scala)
  runs jobs on Flink's mini cluster over the committed sample of a real GH Archive hour, through the
  real file source, so decoding, watermarks, windowing and keyed state are all exercised on data
  nobody wrote to make a test pass.

CI runs the whole thing on Java 11 and 17, checks formatting, builds the deployable jars, and runs
the batch demo.

## Deploying

```bash
sbt spark/assembly flink/assembly
# → modules/{spark,flink}/target/scala-2.12/gh-pulse-{spark,flink}.jar
```

Spark and Flink are `provided`, so the jars are small and never fight the versions already on the
cluster. The live dashboard is the exception: `sbt live/assembly` bundles Flink into one
`gh-pulse-live.jar` that runs with plain `java -jar` and needs no cluster at all (see
[`live/README.md`](live/README.md)).

```bash
spark-submit --class ghpulse.spark.SparkPipeline --master yarn \
  modules/spark/target/scala-2.12/gh-pulse-spark.jar \
  --input hdfs:///data/gharchive/2024-01-15 --output hdfs:///out/gh-pulse

flink run -c ghpulse.flink.FlinkPipeline \
  modules/flink/target/scala-2.12/gh-pulse-flink.jar \
  --input /data/events.jsonl --top 25
```

A one-node Flink cluster, for seeing the job graph and its keyed state in the web UI:

```bash
sbt flink/assembly
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml exec jobmanager \
  flink run -c ghpulse.flink.FlinkPipeline /jars/gh-pulse-flink.jar \
    --input /data/sample/events.jsonl --top 10
# http://localhost:8081
```

## Options

Both entry points take the same flags, parsed by the same code:

```
--input <path>            file, directory or glob of newline-delimited JSON  (required)
--output <path>           write results here instead of printing them
--decoder <name|class>    'gharchive', or your own EventDecoder's class name (default: gharchive)
--top <n>                 leaderboard size                                   (default: 20)
--window <minutes>        activity window width                              (default: 60)
--out-of-orderness <sec>  how late a streaming event may arrive              (default: 300)
--burst-events <n>        events by one actor that make a burst              (default: 10)
--burst-window <minutes>  the interval those events must fall in             (default: 5)
--burst-cooldown <min>    quiet period after reporting a burst               (default: 15)
--weight-<kind> <double>  override a scoring weight; <kind> is one of
                          commit, pr, merged-pr, issue, comment, star, fork
```

## Layout

```
modules/core/     domain model, decoders, pure analytics — no Spark, no Flink
modules/spark/    batch adapters + jobs
modules/flink/    streaming adapters + jobs (incl. the live dashboard job)
modules/live/     packaging only: the self-contained gh-pulse-live.jar
live/             the dashboard: poller, web page, deployment files
data/sample/      1,335 events from a real GH Archive hour (see data/sample/README.md)
scripts/          fetch real data; rebuild the sample
docker/           one-node Flink cluster
```

## Requirements

JDK 11 or 17, and sbt 1.10+ (the live jar also runs on Java 21, Debian 13's default). Java 8 is not supported: the test JVMs need `--add-opens` to reach the
JDK internals Spark and Flink use, and a Java 8 launcher rejects the flag.

## License

MIT — see [LICENSE](LICENSE). GH Archive data is published under the
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) license.
