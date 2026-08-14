# The committed sample

`events.jsonl` is 1,335 events from a real hour of the public [GH Archive] firehose —
`2024-01-15-15.json.gz`, which holds 266,871 events in total. Every 200th record was kept, so the
sample spans all sixty minutes, carries the same mix of event types as the hour it came from, and
arrives slightly out of order exactly as the original does. That last property is the point: it is
what gives the streaming job's watermarks something real to reorder.

The payloads are projected down to the fields the pipelines read (a full pull request payload is
tens of kilobytes of data nothing here looks at), which takes the file from 4.9 MB to 640 KB.
Field names and nesting are untouched, so a full unmodified GH Archive file decodes through exactly
the same path — `scripts/fetch-data.sh` downloads some to try it on.

## Regenerating it

From the repository root:

```bash
curl -sS https://data.gharchive.org/2024-01-15-15.json.gz \
  | gzip -cd | python3 scripts/make-sample.py > data/sample/events.jsonl
```

[GH Archive]: https://www.gharchive.org/
