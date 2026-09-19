package com.anpr.platform.correlate;

import com.anpr.platform.data.CameraRegistry;
import com.anpr.platform.data.DetectionReader;
import com.anpr.platform.data.SampleData;
import com.anpr.platform.data.Watchlist;
import com.anpr.platform.model.AnprEvent;
import com.anpr.platform.model.CoLocationAlertEvent;
import com.anpr.platform.model.Detection;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
}
