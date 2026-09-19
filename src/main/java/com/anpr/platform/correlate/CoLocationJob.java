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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.lang3.ObjectUtils.max;

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
                .filter(event -> event.isWatchlisted)

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
         * The latest window end seen for this incident, or null if we have not alerted on
         * it yet. That null is the whole test, which is why this is a Long and not a long.
         *
         * transient because a ValueState is a handle into a running state backend: it
         * cannot exist on the client where this object is constructed, only on the worker
         * where it runs.
         */
        private transient ValueState<Long> lastWindowEnd;

        /**
         * Not the constructor: this object is built on the client, serialized, shipped,
         * and only then initialised. There is no RuntimeContext until that has happened.
         *
         * OpenContext, not Configuration - the old signature was removed in Flink 2.x, and
         * without @Override the wrong one compiles quietly and simply never runs.
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
            // Read once. Every call to value() hits the state backend, and holding the
            // result in a local also keeps the two branches genuinely exclusive - the
            // stored value changes underneath you the moment update() is called.
            Long lastWindowEndValue = lastWindowEnd.value();

            // Nothing stored means no alert has been raised for this incident yet.
            if(lastWindowEndValue == null) {
                out.collect(alert);
                lastWindowEnd.update(alert.windowEndMillis);
            } else {
                // A later overlapping window reporting the same incident. Stay silent, but
                // remember how far the evidence now reaches - step 4 expires the state
                // from this value. max rather than plain assignment because windows are
                // not guaranteed to arrive in end-time order above parallelism 1.
                lastWindowEnd.update(max(lastWindowEndValue, alert.windowEndMillis));
            }
        }
    }
}
