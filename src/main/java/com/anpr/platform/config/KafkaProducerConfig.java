package com.anpr.platform.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * The producer settings both publishers use. Everything not set here is a Kafka default,
 * and the defaults that matter are already safe: acks=all and enable.idempotence=true.
 */
public final class KafkaProducerConfig {

    /** Properties, not ProducerConfig: the constants name the keys, they don't build the object. */
    public static Properties properties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaTopics.BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private KafkaProducerConfig() {
    }
}
