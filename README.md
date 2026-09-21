# Vehicle-Event-Processing

Vehicle detection events from ANPR cameras, matched against a watchlist of vehicles of
interest, producing two kinds of alert:

- **`SINGLE_MATCH`** — a watchlisted plate was seen.
- **`CO_LOCATION`** — two or more *different* watchlisted vehicles were seen at the same
  location within 15 minutes.

## Running it

Needs **Docker Desktop** and **JDK 17**. Maven is not required — use the wrapper. NiFi
brings its own Java 21 inside its container, so the two never meet.

Every command is run from the project root; the sample-data paths are relative.

### 1. Start the stack

```powershell
docker compose up -d
```

Kafka, the topic creator, and NiFi. NiFi needs two to three minutes before it answers —
it looks hung and is not.

### 2. Load the flows into NiFi

Open **https://localhost:8443/nifi/** (accept the self-signed certificate) and log in with
`admin` / `anpr-demo-password`.

The canvas starts empty — the flows live in this repository, not in the image. There are
two, one per NiFi stage:

1. Drag a **Process Group** onto the canvas and **Browse** to `nifi/anpr-ingest-flow.json`.
2. Drag a second **Process Group** and **Browse** to `nifi/anpr-route-deliver-flow.json`.
3. Open each group in turn and **enable every controller service inside it** — readers,
   writers, the two CSV lookups and the Kafka connection. Imported services arrive
   disabled, and a processor whose service is disabled reports itself invalid without
   saying which one.
4. Start every processor in both groups.

Both flows carry their own copy of `Kafka3ConnectionService`, so each group is
self-contained and the import order does not matter. Enable the service *inside* each
group rather than looking for one shared between them.

**Start route & deliver before step 4.** Its `ConsumeKafka` reads from `latest`, so alerts
published while it is stopped are never delivered to disk — the same live-edge rule as the
Flink job.

### 3. Start the Flink job

```powershell
.\mvnw.cmd -q clean test
.\mvnw.cmd -q dependency:build-classpath "-Dmdep.outputFile=target/cp.txt"
java -cp "target/classes;$(Get-Content target/cp.txt)" com.anpr.platform.correlate.CoLocationJob
```

Leave it running. **Start it before the next step** — the job reads from the live edge of
the topic, so anything published before it is up is never seen.

`clean` deletes `target/cp.txt`, so rebuild the classpath after any `clean`.

### 4. Trigger the pipeline

In another terminal, replay the feed as the cameras would send it:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run-cameras.ps1
```

The script drops one detection per file into `ingest/`, in timestamp order, three seconds
apart, and prints each one as it goes. `-ExecutionPolicy Bypass` applies to this one run
only; it does not change the machine's policy.

Or drop the whole file at once:

```powershell
Copy-Item sample-data\detections.csv ingest\
```

Both give the same result. `GetFile` consumes what lands in `ingest/`; `sample-data` is
mounted read-only so the originals cannot be eaten.

### 5. What should happen

Twelve detections in, eight alerts out:

```powershell
docker exec anpr-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic anpr-events
docker exec anpr-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic alerts
```

| topic | count | what it is |
|-------|-------|------------|
| `anpr-events` | **+12** | every valid detection, enriched — including the six that carry no alert |
| `alerts` | **+6** | `SINGLE_MATCH`, raised by NiFi, never touched by Flink |
| `alerts` | **+2** | `CO_LOCATION`, from the Flink job |

The Flink console prints the two it found:

```
ALERT  CO_LOCATION  [KE555ZT, NIT77AB]    at LOC-RING     seen by [CAM-01, CAM-04]
ALERT  CO_LOCATION  [TT9999OA, TT9999OP]  at LOC-D1-E12   seen by [CAM-02]
```

The first is the interesting one: two different watchlisted vehicles, five minutes apart,
at **two different cameras that share a location**. That is the rule doing something no
single record could answer.

### Where the alerts land

Route & deliver splits the `alerts` topic by type and writes one file per alert:

```
data-out/alerts/
├── co-location/
│   ├── 0805_LOC-RING_KE555ZT-NIT77AB.json
│   └── 0856_LOC-D1-E12_TT9999OP-TT9999OA.json
└── single-match/
    ├── 0813_BA123XY_d-1002.json    0841_KE555ZT_d-1008.json
    ├── 0814_KE555ZT_d-1003.json    0910_TT9999OA_d-1011.json
    └── 0819_NIT77AB_d-1005.json    0910_TT9999OP_d-1010.json
```

```powershell
Get-ChildItem -Recurse data-out\alerts | Select-Object Directory, Name
```

The directory is the fan-out and the filename is the summary, so `ls` answers "what
happened" without opening anything. Single-match names lead with the detection's own
timestamp; co-location names lead with the **window start**, because that alert's evidence
is an interval rather than an instant.

Alerts name places by `locationId` — `LOC-RING`, not *Ring Road / Mlynske Nivy*. `data-out/`
is where this pipeline hands off, and turning an ID into a display name belongs to whatever
shows the alerts to a person. `sample-data/locations.csv` is that mapping; nothing in the
pipeline reads it.

`data-out/` is a bind mount declared in `docker-compose.yml` — NiFi writes to
`/opt/nifi/data-out` inside the container and the same files appear here. The directory is
git-ignored; only `.gitkeep` is tracked.

Writes use `replace` conflict resolution, so re-running the demo overwrites rather than
failing. Re-running produces the six `SINGLE_MATCH` files again but **no** new
`CO_LOCATION` unless the Flink job is restarted first. The job's watermark has already
moved past those events, so the second copy arrives late and is dropped before any window
sees it. A restart clears the watermark, because the job keeps no checkpoints.

### Why twelve events and not six

Flink discards non-watchlisted events in its first operator, so forwarding them looks
wasteful. It is not. Watermarks are derived from the timestamps of arriving records, and
the last row in `detections.csv` — `d-1012`, an unwatchlisted plate fifteen minutes after
everything else — exists only to push event time past the final window so it can close.
Filter those rows out at ingest and the second `CO_LOCATION` alert never appears.

A heartbeat implemented in data. See *Known limitations*.

### A second feed

`sample-data/detections-b.csv` is ten more detections, 09:40–10:15 — every one later than
the last event in `detections.csv`, so it can follow the first run without restarting the
Flink job. It exercises three things the first file never does:

| rows | what they show |
|------|----------------|
| `d-2002`, `d-2004` | `CO_LOCATION` at `LOC-OLDTOWN` — two watchlisted plates at the **same** camera |
| `d-2006`, `d-2007` | `CO_LOCATION` at `LOC-RING` — two plates on **two** cameras that share one location |
| `d-2008` | `CAM-09` is not in `cameras.csv`, so the detection is dead-lettered and never published |

Two near-misses are there on purpose. `d-2001` and `d-2009` share `LOC-D1-E12`, but
`ZA482KL` is not watchlisted, so there is no pair. And `d-2010` puts `MN667PL` at
`LOC-RING` 16 minutes after `d-2007` — one minute outside the window. It also serves as the
heartbeat that moves event time past the last window so it can close.

```powershell
Copy-Item sample-data\detections-b.csv ingest\
```

| where | expect |
|-------|--------|
| `anpr-events` | **+9** — ten rows, minus `d-2008` |
| `alerts` | **+6** `SINGLE_MATCH`, **+2** `CO_LOCATION` |
| `data-out/alerts/co-location/` | `…_LOC-OLDTOWN_BA123XY-KE555ZT.json`, `…_LOC-RING_TT9999OP-NIT77AB.json` |
| ingest canvas | one flowfile queued on the camera lookup's `unmatched` funnel |

**Run the two files one after the other, never together.** Dropped at once, NiFi does not
guarantee which it publishes first. If the second file reaches Kafka first, the watermark
jumps to 10:15, and every row of the first file then arrives late and is dropped — its
two `CO_LOCATION` alerts never fire.

### Resetting

```powershell
docker compose down; docker compose up -d
```

Kafka declares no volumes, so this empties both topics. NiFi's canvas is in a named volume
and survives.

**Do not add `-v`.** That deletes the named volumes too, and your imported flow with them.

### In Git Bash instead of PowerShell

Use `$(cat target/cp.txt)` for the classpath, and prefix every `docker exec` that names a
container path with `MSYS_NO_PATHCONV=1`, or the `/opt/...` argument is rewritten into a
Windows path before Docker sees it.

## Architecture

```
ANPR cameras
    |
    |  detections: plate, camera, timestamp, confidence
    v
NiFi  [ingest & enrich] ------------------------- SINGLE_MATCH --+
    |                                                            |
    |  enriched events: + locationId, + watchlisted              |
    v                                                            |
Kafka  "anpr-events"                                             |
    |                                                            |
    v                                                            |
Flink  [correlate] ------------------------------ CO_LOCATION ---+
          keyBy(locationId), sliding event-time window           |
          (15 min / 1 min), one alert per incident               |
                                                                 v
                                                        Kafka  "alerts"
                                                                 |
                                                                 v
                                                  NiFi  [route & deliver]
                                                        by "type", onward
```

Two sentences carry the design:

**State over time is the only thing that needs a stream processor, so that is the only
thing Flink does.** Everything else is routing and buffering, and those have better tools.

**Kafka carries everything.** Both alert types go onto the `alerts` topic, including the
one Flink never sees — so there is one replayable record of everything the system has ever
raised, and one path to delivery.

### ANPR cameras

The raw fact: this plate, at this camera, at this instant. No interpretation. Everything
downstream is derived from it, so it is the only thing in the system that is evidence
rather than conclusion.

The sample CSVs stand in for them: drop one into `ingest/` and NiFi takes it from there.
There is no second way in — every detection enters the system through NiFi.

### NiFi — ingest & enrich

Three jobs: **validate** (a detection from an unknown camera has no location, so it is
dead-lettered rather than guessed), **enrich** (camera → location, plate → watchlisted),
and **raise `SINGLE_MATCH` on the spot** — published straight to the `alerts` topic,
never reaching Flink.

Both lookups are `LookupRecord` stages against the same CSVs the Java code reads. The
watchlist lookup's `matched`/`unmatched` split is what produces the `watchlisted` flag,
and doubles as the routing that sends matched records on to `alerts`.

Why NiFi rather than Flink: a single watchlisted plate needs no memory of anything. One
record in, one decision out. That is a routing problem, and routing with retries,
provenance and back-pressure is what NiFi is built for. Answering it in Flink would mean
standing up a distributed stateful engine for a question two CSV lookups answer in one
hop — and it would mean Flink needing the watchlist, which is the complexity described
under *Correlate* below.

Enrichment happens here because `SINGLE_MATCH` forces the watchlist lookup at this point
anyway. Once it is done, carrying the answer forward costs nothing; redoing it downstream
does.

### Kafka — `anpr-events`

The buffer, and the seam. It does three things nothing else in the design does:

- **Decouples lifetimes.** NiFi keeps publishing while Flink is down, and Flink keeps
  running while NiFi restarts. One trade-off is deliberate: the job starts at the live edge
  of the topic, so events published while it is down are not correlated when it comes back.
  It reports what is happening now, not a backlog.
- **Absorbs bursts.** Rush hour does not back-pressure the cameras.
- **Keeps the record.** Every event stays on the topic, so replay is available, though not
  wired up here. Started from an earlier offset with fresh state, the job would recompute
  the same alerts, because correlation runs on event time rather than arrival time.

That last property is what makes the system testable rather than merely runnable.

### Flink — correlate

The only part of the system that has to remember things. *"Two different watchlisted
vehicles at one location within 15 minutes"* cannot be answered from one record. It needs
every record for that location across a moving span of time, keyed by location, and
windowed by **when the cameras saw the vehicles** — not when the data happened to arrive.

That is four capabilities at once: keyed state, event time, watermarks and timers. Flink
is in this design for that rule and nothing else. The job does not validate, enrich or
look anything up; `filter(watchlisted)` is an early discard, not a decision.

The job trusts the `watchlisted` flag on the wire rather than re-checking it. Re-checking
would require the watchlist inside Flink as broadcast state — a second stream of watchlist
updates connected to the main stream — which is the single largest complexity this design
could take on, in order to re-derive something upstream already computed. See *Known
limitations*.

### Kafka — `alerts`

The same argument applied to output: detection is separated from delivery. Flink's job
ends when it has decided something is true. Whether that becomes an email, a case file or
a dashboard row is someone else's problem, and that someone can change, fail, or be added
to without touching the correlation logic.

Both alert types land here, including `SINGLE_MATCH`, which never needed Flink. Routing it
through Kafka costs one hop of latency and buys a single audit log — every alert the system
has raised, replayable, in one place. For an investigative platform, *"what did we alert on
last Tuesday, and why"* should be one command, not a trawl through NiFi provenance.

Two shapes on one topic, so each carries a discriminator:

```json
{"type":"CO_LOCATION","locationId":"LOC-RING","plates":["KE555ZT","NIT77AB"],
 "cameraIds":["CAM-01","CAM-04"],
 "windowStartMillis":1758182700000,"windowEndMillis":1758183600000}
```

### NiFi — route & deliver

Fan-out on `type`, which is exactly what `RouteOnAttribute` is for. One input, one router,
two destinations — which is what makes the two alert types one system rather than two.

It delivers what it receives and adds nothing. Location names stay out of the files for the
same reason they stay out of Kafka: an ID is stable and a name is display text, so it is
resolved by the consumer from `sample-data/locations.csv`.

## Code layout

Layering runs `correlate` → `{serde, data, config}` → `model`.

| package     | what it holds                                                                 |
|-------------|-------------------------------------------------------------------------------|
| `model`     | `Detection`, `AnprEvent`, the alert types. No framework imports at all, so the domain does not depend on the plumbing. |
| `data`      | `Watchlist` and `CameraRegistry`, the lookups the prototype runs against. Narrow on purpose — `contains()` and `locationIdOf()`, nothing enumerable — so a database-backed version only has to build one through `of()`. |
| `serde`     | The JSON that travels on the topics, pinned by tests rather than by schema strictness. |
| `correlate` | Two implementations of one rule: a plain-Java prototype whose tests are the spec, and the Flink job that has to match it. |
| `config`    | Topic names and the broker address the Flink job connects to. |

The plain-Java prototype stays deliberately. It documents what the rule is without any
framework in the way, and it is the reason the Flink job can be read as *"the same rule,
but able to remember"*.

## Known limitations

- **The watchlist flag is point-in-time.** A plate added to the watchlist at 10:00 does not
  retroactively flag events published before 10:00, even if a window covering them is
  still open. Acceptable for an investigative platform, which cares about vehicles of
  interest going forward; the production answer is broadcast state in the Flink job.
- **An alert cannot be raised until event time has passed the window.** Watermarks are
  derived from the data, so a stream that goes quiet reports nothing about its final
  window. The production answer is a heartbeat from ingest, so event time advances even
  when no camera sees anything.
