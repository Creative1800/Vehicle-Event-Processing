package com.anpr.platform.app;

import com.anpr.platform.config.KafkaProducerConfig;
import com.anpr.platform.config.KafkaTopics;
import com.anpr.platform.data.Enricher;
import com.anpr.platform.data.SampleVocabulary;
import com.anpr.platform.model.AnprEvent;
import com.anpr.platform.model.Detection;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;

/**
 * A live camera feed. Runs until Ctrl+C.
 *
 * Emits what a camera actually sees - a plate, at a camera, at a time, with a confidence
 *
 * Event time runs faster than the wall clock: SIM_STEP of simulated time passes every TICK
 * of real time, so a 15 minute window closes in a few seconds. The Flink job is untouched
 * by this - it still uses real event time and real 15 minute windows. Only the cameras are
 * sped up, which is what a simulator is for.
 *
 */
public final class CameraSimulator {

    /** Simulated time added per event. With TICK below, 5 simulated minutes per real second. */
    private static final Duration SIM_STEP = Duration.ofSeconds(30);

    /** Real time between events. */
    private static final Duration TICK = Duration.ofMillis(100);

    /** A deliberate co-location every this many events - roughly one alert every 6 seconds. */
    private static final int EVENTS_PER_BURST = 60;

    /** Fixed seed: a live feed that is still the same feed every run. */
    private static final long SEED = 42L;

    /** How long the shutdown hook waits for buffered records before giving up. */
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(5);

    private static final double MIN_CONFIDENCE = 0.85;
    private static final double MAX_CONFIDENCE = 0.99;

    public static void main(String[] args) throws Exception {

        SampleVocabulary vocabulary = SampleVocabulary.load();
        Enricher enricher = Enricher.fromSampleData();
        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random(SEED);

        // Starts at now, so the feed is always ahead of the sample CSVs and the two
        // publishers can feed the same job without the simulator arriving as late data.
        Instant simNow = Instant.now();
        long eventCount = 0;

        Producer<String, String> producer = new KafkaProducer<>(KafkaProducerConfig.properties());
        closeOnShutdown(producer);

        System.out.println("Feeding " + KafkaTopics.ANPR_EVENTS + " from " + simNow + ". Ctrl+C to stop.");

        while (true) {
            if (eventCount % EVENTS_PER_BURST == 0) {
                // Two different watchlisted vehicles at one camera, one SIM_STEP apart -
                // well inside the 15 minute window, so this is what the job must catch.
                // Picked from the watchlisted pool, but still only FLAGGED if the lookup
                // agrees: a generator does not get to decide what is wanted.
                String cameraId = pick(random, vocabulary.cameraIds());
                String first = pick(random, vocabulary.watchlistedPlates());
                String second = pickOtherThan(random, vocabulary.watchlistedPlates(), first);

                send(producer, mapper, enricher,
                        detection(++eventCount, first, cameraId, simNow, random));
                simNow = simNow.plus(SIM_STEP);
                send(producer, mapper, enricher,
                        detection(++eventCount, second, cameraId, simNow, random));

                System.out.println("  burst  " + first + " + " + second + " at " + cameraId + "  " + simNow);
            } else {
                // Background traffic. Filtered out by the job, but it is what keeps the
                // watermark advancing - the heartbeat, in the form of real data.
                String plate = pick(random, vocabulary.otherPlates());
                String cameraId = pick(random, vocabulary.cameraIds());

                send(producer, mapper, enricher,
                        detection(++eventCount, plate, cameraId, simNow, random));
            }

            simNow = simNow.plus(SIM_STEP);
            Thread.sleep(TICK.toMillis());
        }
    }

    /** Everything a camera knows: a plate, itself, the time, and how sure it is. */
    private static Detection detection(long sequence, String plate, String cameraId,
                                       Instant simNow, Random random) {
        double confidence = MIN_CONFIDENCE + random.nextDouble() * (MAX_CONFIDENCE - MIN_CONFIDENCE);
        return new Detection("sim-" + sequence, plate, cameraId, simNow, confidence);
    }

    /**
     * Enrich, then publish. The enrichment step is NiFi's job, done here until NiFi exists,
     * which is why it is one visible line rather than folded into the generator.
     *
     * Fire and forget, unlike the one-shot publisher. An infinite run has no moment at
     * which it reports success, so blocking on every send buys nothing; the callback is
     * there so a broker failure is still visible.
     */
    private static void send(Producer<String, String> producer, ObjectMapper mapper,
                             Enricher enricher, Detection detection) throws Exception {
        AnprEvent event = enricher.enrich(detection);
        if (event == null) {
            return;
        }

        String json = mapper.writeValueAsString(event);

        producer.send(new ProducerRecord<>(KafkaTopics.ANPR_EVENTS, event.locationId, json),
                (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Publish failed: " + exception);
                    }
                });
    }

    /**
     * Ctrl+C kills the JVM mid-loop, and an infinite run never reaches a close(). Without
     * this the last batch sits unflushed in the producer buffer and is lost. close()
     * flushes first, so there is no separate flush() call.
     */
    private static void closeOnShutdown(Producer<String, String> producer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close(DRAIN_TIMEOUT);
            System.out.println("Stopped. Buffered records flushed.");
        }));
    }

    private static String pick(Random random, List<String> values) {
        return values.get(random.nextInt(values.size()));
    }

    private static String pickOtherThan(Random random, List<String> values, String excluded) {
        String picked;
        do {
            picked = pick(random, values);
        } while (picked.equals(excluded));
        return picked;
    }

    private CameraSimulator() {
    }
}
