package com.anpr.platform.correlate;

import com.anpr.platform.data.CameraRegistry;
import com.anpr.platform.data.DetectionReader;
import com.anpr.platform.data.SampleData;
import com.anpr.platform.data.Watchlist;
import com.anpr.platform.model.AnprEvent;
import com.anpr.platform.model.CoLocationAlertEvent;
import com.anpr.platform.model.Detection;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;
import java.util.*;


/**
 * The Flink half of the design: CO_LOCATION alerts, the only alert that needs state over
 * time. SINGLE_MATCH never reaches here - NiFi raises it on the spot.
 *
 * Reads a bounded in-memory source for now. The Kafka source replaces exactly one line.
 */
public final class CoLocationJob {

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(15);
    private static final Duration WINDOW_SLIDE = Duration.ofMinutes(1);

    /** How long a watermark waits for stragglers before declaring a window complete. */
    private static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(30);

    private static final int MIN_DISTINCT_PLATES = 2;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parallelism 1 only so the printed output is in a readable order. keyBy would be
        // correct at any parallelism - locations simply spread across more subtasks.
        env.setParallelism(1);

        WatermarkStrategy<AnprEvent> watermarks = WatermarkStrategy
                .<AnprEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, recordTimestamp) -> event.eventTimeMillis);

        env.fromData(enrichedSampleEvents())

                // Event time, not processing time: a replay of old data must still produce
                // the alerts that data implies, whatever the wall clock says.
                .assignTimestampsAndWatermarks(watermarks)

                // NiFi already decided this. Filtering early keeps uninteresting traffic
                // out of the windows entirely.
                .filter(event -> event.watchlisted)

                // Types.STRING is not decoration: a Java lambda loses its generic types to
                // erasure, so Flink cannot always infer the key type. Stating it avoids a
                // startup failure that reads like a compiler bug.
                .keyBy(event -> event.locationId, Types.STRING)

                // A fresh 15 minute window every minute. Tumbling windows would miss a pair
                // seen at 08:14 and 08:16, because a fixed bucket boundary falls between
                // them. The cost is that one event belongs to ~15 windows.
                .window(SlidingEventTimeWindows.of(WINDOW_SIZE, WINDOW_SLIDE))

                .process(new CoLocationWindow())

                // The stream stops being keyed by LOCATION here and starts being keyed by
                // INCIDENT. Every overlapping window that reported the same plates at the
                // same place collapses onto one key, which is what makes the repeats
                // recognisable as repeats.
                .keyBy(CoLocationAlertEvent::dedupeKey)

                // Flink 2.2 builds this operator on the async state backend, so it has to
                // be handed the matching store explicitly - without this the state lookup
                // in open() lands in the synchronous store and is refused at startup. It
                // also fixes the state API: v2 descriptors, not v1.
                .enableAsyncState()

                // Ensures the alert is emitted only once per incident
                .process(new EmitOncePerIncident())

                .print();

        env.execute("ANPR co-location correlation");
    }

    /**
     * Evaluated once per location per window, with every event that fell inside it.
     *
     * The prototype had to ask "how far back does this reach?" on every detection. Here
     * the window already IS the answer, so this only has to count distinct plates.
     */
    private static final class CoLocationWindow
            extends ProcessWindowFunction<AnprEvent, CoLocationAlertEvent, String, TimeWindow> {

        @Override
        public void process(String locationId,
                            Context context,
                            Iterable<AnprEvent> events,
                            Collector<CoLocationAlertEvent> out) {

            Set<String> plates = new LinkedHashSet<>();
            Set<String> cameras = new LinkedHashSet<>();

            for (AnprEvent event : events) {
                plates.add(event.plate);
                cameras.add(event.cameraId);
            }

            if (plates.size() < MIN_DISTINCT_PLATES) {
                return;
            }

            out.collect(new CoLocationAlertEvent(
                            locationId,
                            plates,
                            cameras,
                            context.window().getStart(),
                            context.window().getEnd()));
        }
    }

    /**
     * Stands in for NiFi: reads the sample CSVs and does the enrichment NiFi will do -
     * resolve the camera to a location, and flag whether the plate is watchlisted.
     *
     * This whole method disappears when Kafka arrives; a KafkaSource replaces it.
     */
    private static List<AnprEvent> enrichedSampleEvents() throws IOException {
        Watchlist watchlist = Watchlist.loadFrom(SampleData.WATCHLIST);
        CameraRegistry cameras = CameraRegistry.loadFrom(SampleData.CAMERAS, SampleData.LOCATIONS);

        List<AnprEvent> events = new ArrayList<>();

        for (Detection detection : DetectionReader.readAll(SampleData.DETECTIONS)) {
            String locationId = cameras.locationIdOf(detection.cameraId());
            if (locationId == null) {
                continue;
            }

            events.add(new AnprEvent(
                    detection.id(),
                    detection.plate(),
                    detection.cameraId(),
                    locationId,
                    watchlist.contains(detection.plate()),
                    detection.timestamp().toEpochMilli()));
        }

        return events;
    }

    /**
     * Lets the FIRST alert for an incident through and swallows every later window that
     * reports the same one. This is what turns 25 printed alerts into 2.
     *
     * Keyed by incident rather than by location - see the second keyBy above. The window
     * operator decides whether plates were co-located; this one decides whether we have
     * already said so.
     */
    private static final class EmitOncePerIncident
            extends KeyedProcessFunction<String, CoLocationAlertEvent, CoLocationAlertEvent> {

        /**
         * Latest window end seen for this incident, null if none yet - that null is the
         * test, hence Long rather than long. transient because a ValueState only exists on
         * the worker, and per-incident data cannot be a plain field: one instance of this
         * class serves every incident.
         */
        private transient ValueState<Long> lastWindowEnd;

        /**
         * Not the constructor: this object is built on the client and shipped, so there is
         * no RuntimeContext until it starts. OpenContext, not Configuration - the old
         * signature was removed in Flink 2.x.
         */
        @Override
        public void open(OpenContext openContext) {
            lastWindowEnd = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("lastWindowEnd", Long.class));
        }

        @Override
        public void processElement(CoLocationAlertEvent alert,
                                   Context context,
                                   Collector<CoLocationAlertEvent> out) throws Exception {

            Long lastSeen = lastWindowEnd.value();

            if (lastSeen == null) {
                // no alert has been raised for this incident yet.
                out.collect(alert);
                lastWindowEnd.update(alert.windowEndMillis);
                context.timerService().registerEventTimeTimer(expiryOf(alert.windowEndMillis));
                return;
            }

            long extended = Math.max(lastSeen, alert.windowEndMillis);

            if (extended == lastSeen) {
                // Evidence reaches no further, so the standing timer is still the right one.
                return;
            }

            // Delete before registering: Flink keeps two timers at different timestamps,
            // and the earlier one would clear the state mid-incident.
            context.timerService().deleteEventTimeTimer(expiryOf(lastSeen));
            lastWindowEnd.update(extended);
            context.timerService().registerEventTimeTimer(expiryOf(extended));
        }

        /**
         * Fires once no further window can report this incident. Clearing rather than
         * marking it alerted: the same plates meeting again later is a new incident.
         * Emits nothing - the alert went out on first sighting.
         */
        @Override
        public void onTimer(long timestamp,
                            OnTimerContext context,
                            Collector<CoLocationAlertEvent> out) throws Exception {
            lastWindowEnd.clear();
        }

        /**
         * When an incident may be forgotten: one window after the last window that
         * reported it. The extra window is margin - a timer at the window end itself
         * races that window's own output. Register and delete both call this, so their
         * timestamps cannot drift apart.
         */
        private static long expiryOf(long windowEndMillis) {
            return windowEndMillis + WINDOW_SIZE.toMillis();
        }
    }
}
