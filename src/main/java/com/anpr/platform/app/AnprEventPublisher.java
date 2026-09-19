package com.anpr.platform.app;

import com.anpr.platform.config.KafkaProducerConfig;
import com.anpr.platform.model.AnprEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

import static com.anpr.platform.config.KafkaTopics.ANPR_EVENTS;
import static com.anpr.platform.data.EnrichedSampleEvents.enrichedSampleEvents;

/**
 * Stands in for NiFi's ingest half: enriches the sample detections and publishes them as
 * JSON to "anpr-events". Publishes everything, watchlisted or not - the filter belongs to
 * the correlation job, exactly as it would with the real NiFi flow in front.
 */
public final class AnprEventPublisher {

    public static void main(String[] args) throws Exception {

        // Read the CSVs before opening a producer: a missing file should fail first.
        List<AnprEvent> events = enrichedSampleEvents();

        // One mapper for the whole run - thread-safe, and expensive to build.
        ObjectMapper mapper = new ObjectMapper();

        int published = 0;

        try (Producer<String, String> producer = new KafkaProducer<>(KafkaProducerConfig.properties())) {
            for (AnprEvent event : events) {
                String json = mapper.writeValueAsString(event);

                // Keyed by location so a location's events share a partition and stay in
                // order. .get() per record: slow, but a failed publish must fail the run.
                producer.send(new ProducerRecord<>(ANPR_EVENTS, event.locationId, json)).get();
                published++;
            }
        }

        System.out.println("Published " + published + " events to " + ANPR_EVENTS);
    }

    private AnprEventPublisher() {
    }
}
