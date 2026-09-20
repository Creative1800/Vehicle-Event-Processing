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

### 2. Load the flow into NiFi

Open **https://localhost:8443/nifi/** (accept the self-signed certificate) and log in with
`admin` / `anpr-demo-password`.

The canvas starts empty — the flow lives in this repository, not in the image:

1. Drag a **Process Group** onto the canvas and **Browse** to `nifi/anpr-ingest-flow.json`.
2. Open the imported group, then **enable every controller service** in it — readers,
   writers, the two CSV lookups and the Kafka connection. Imported services arrive
   disabled, and a processor whose service is disabled reports itself invalid without
   saying why.
3. Start every processor.

### 3. Start the Flink job

```powershell
.\mvnw.cmd -q clean test
.\mvnw.cmd -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "target/classes;$(Get-Content target/cp.txt)" com.anpr.platform.correlate.CoLocationJob
```

Leave it running. **Start it before the next step** — the job reads from the live edge of
the topic, so anything published before it is up is never seen.

`clean` deletes `target/cp.txt`, so rebuild the classpath after any `clean`.

### 4. Trigger the pipeline

In another terminal:

```powershell
Copy-Item sample-data\detections.csv ingest\
```

`GetFile` consumes the copy; `sample-data` is mounted read-only so the originals cannot be
eaten.

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

### Why twelve events and not six

Flink discards non-watchlisted events in its first operator, so forwarding them looks
wasteful. It is not. Watermarks are derived from the timestamps of arriving records, and
the last row in `detections.csv` — `d-1012`, an unwatchlisted plate fifteen minutes after
everything else — exists only to push event time past the final window so it can close.
Filter those rows out at ingest and the second `CO_LOCATION` alert never appears.

A heartbeat implemented in data. See *Known limitations*.

### Live mode

For a continuously running feed instead of the fixed twelve:

```powershell
java -cp "target/classes;$(Get-Content target/cp.txt)" com.anpr.platform.app.CameraSimulator
```

Thirty seconds of event time every 100 ms, so a fifteen-minute window closes in about
three seconds. Every sixtieth event it stages two different watchlisted plates at one
camera, producing a `CO_LOCATION` alert every few seconds. Ctrl+C to stop.

This one bypasses NiFi and publishes to `anpr-events` directly — it exists to watch the
correlation logic run, not to demonstrate the pipeline.

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

The reproducible run is the pipeline itself: drop `sample-data/detections.csv` into
`ingest/` and NiFi produces the twelve enriched events. `CameraSimulator` stands in for a
live feed — continuous, synthetic, for watching the system run rather than demonstrating
it.

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

- **Decouples lifetimes.** NiFi can restart while Flink runs, and the reverse. The
  publisher can finish and exit; the job can start an hour later and still see everything.
- **Absorbs bursts.** Rush hour does not back-pressure the cameras.
- **Makes replay possible.** The topic is the record. Re-run the job over the same offsets
  and, because correlation is by event time, the alerts are identical.

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

## Code layout

Layering runs `app` → `correlate` → `{serde, data, config}` → `model`.

| package     | what it holds                                                                 |
|-------------|-------------------------------------------------------------------------------|
| `model`     | `Detection`, `AnprEvent`, the alert types. No framework imports at all, so the domain does not depend on the plumbing. |
| `data`      | `Watchlist`, `CameraRegistry`, the CSV readers. Narrow on purpose — `contains()` and `locationIdOf()`, nothing enumerable — so moving the watchlist into a database rewrites one method. |
| `serde`     | The JSON that travels on the topics, pinned by tests rather than by schema strictness. |
| `correlate` | Two implementations of one rule: a plain-Java prototype whose tests are the spec, and the Flink job that has to match it. |
| `app`       | `CameraSimulator`, which bypasses NiFi to publish straight to `anpr-events` — a load generator for the Flink job, not an ingest path. The two console demos alongside it print alerts without Kafka. |
| `config`    | Topic names and producer settings, kept out of the code that uses them so the topic names have one home. |

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
