package com.anpr.platform.app;

import com.anpr.platform.model.AnprEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

import static com.anpr.platform.config.KafkaTopics.ANPR_EVENTS;
import static com.anpr.platform.config.KafkaTopics.BOOTSTRAP_SERVERS;
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

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfig())) {
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

    /** Properties, not ProducerConfig: the constants name the keys, they don't build the object. */
    private static Properties producerConfig() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private AnprEventPublisher() {
    }
}
