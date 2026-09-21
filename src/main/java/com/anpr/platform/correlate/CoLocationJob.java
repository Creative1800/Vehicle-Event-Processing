package com.anpr.platform.correlate;

import com.anpr.platform.config.KafkaTopics;
import com.anpr.platform.model.AnprEvent;
import com.anpr.platform.model.CoLocationAlertEvent;
import com.anpr.platform.serde.AnprEventDeserializationSchema;
import com.anpr.platform.serde.CoLocationAlertSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;


/**
 * The Flink half of the design: CO_LOCATION alerts, the only alert that needs state over
 * time. SINGLE_MATCH never reaches here - NiFi raises it on the spot.
 */
public final class CoLocationJob {

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(15);
    private static final Duration WINDOW_SLIDE = Duration.ofMinutes(1);

    /** How long a watermark waits for stragglers before declaring a window complete. */
    private static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(30);

    private static final int MIN_DISTINCT_PLATES = 2;

    private static final String CONSUMER_GROUP = "colocation-job";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parallelism 1 only so the printed output is in a readable order. keyBy would be
        // correct at any parallelism - locations simply spread across more subtasks.
        env.setParallelism(1);

        KafkaSource<AnprEvent> source = anprEventSource();

        WatermarkStrategy<AnprEvent> watermarks = WatermarkStrategy
                .<AnprEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, recordTimestamp) -> event.eventTimeMillis);

        DataStream<CoLocationAlertEvent> alerts = env.fromSource(source, watermarks, KafkaTopics.ANPR_EVENTS)
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

                // Report on arrival, not when the window closes - see FireOnEveryEvent.
                .trigger(new FireOnEveryEvent())

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
                .process(new EmitOncePerIncident());

        // Two sinks on one stream: the console is for the demo, Kafka is the delivery path.
        alerts.print();
        alerts.sinkTo(alertSink());

        env.execute("ANPR co-location correlation");
    }

    /**
     * Fires the window on every event, instead of once when the watermark passes its end.
     *
     * Safe because a co-location can only be confirmed, never undone: later events add
     * plates, they never take one away. So there is nothing to wait for - the alert goes
     * out the moment the second plate arrives, with no out-of-orderness delay and no need
     * for a later event to close the window.
     *
     * FIRE, not FIRE_AND_PURGE: the window keeps its contents, so the next event is judged
     * against everything seen so far. Every open window holding a pair re-reports it on
     * each event; EmitOncePerIncident swallows the repeats.
     */
    private static final class FireOnEveryEvent extends Trigger<Object, TimeWindow> {

        @Override
        public TriggerResult onElement(Object event,
                                       long timestamp,
                                       TimeWindow window,
                                       TriggerContext context) {
            // The window's end, so onEventTime gets a chance to drop its contents.
            context.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.FIRE;
        }

        /** Purge without firing: everything in this window has already been reported. */
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext context) {
            return time == window.maxTimestamp() ? TriggerResult.PURGE : TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext context) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext context) {
            context.deleteEventTimeTimer(window.maxTimestamp());
        }
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
     * Lets the FIRST alert for an incident through and swallows every later window that
     * reports the same one. This is what turns dozens of printed alerts into 2.
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

    private static KafkaSource<AnprEvent> anprEventSource() {
        return KafkaSource.<AnprEvent>builder()
            .setBootstrapServers(KafkaTopics.BOOTSTRAP_SERVERS)
            .setTopics(KafkaTopics.ANPR_EVENTS)
            .setGroupId(CONSUMER_GROUP)
            // Start at the live edge: a running job reports what is happening now,
            // not a replay of what was already in the topic.
            .setStartingOffsets(OffsetsInitializer.latest())
            // Only the value matters - event time comes from inside the JSON,
            // not from the Kafka record's key or timestamp.
            .setValueOnlyDeserializer(new AnprEventDeserializationSchema())
            .build();
    }

    private static KafkaSink<CoLocationAlertEvent> alertSink() {
        return KafkaSink.<CoLocationAlertEvent>builder()
            .setBootstrapServers(KafkaTopics.BOOTSTRAP_SERVERS)
            .setRecordSerializer(KafkaRecordSerializationSchema.<CoLocationAlertEvent>builder()
                .setTopic(KafkaTopics.ALERTS)
                // No record key: one partition, and any keying scheme has to cover
                // NiFi's SINGLE_MATCH too. Deferred until that shape exists.
                .setValueSerializationSchema(new CoLocationAlertSerializationSchema())
                .build())
            // EXACTLY_ONCE would need checkpointing plus a transactional id prefix, and
            // would hold alerts back until each checkpoint commits. A repeat after a
            // restart is a nuisance, not a wrong answer.
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }
}
