package com.anpr.platform.config;

public final class KafkaTopics {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String ANPR_EVENTS = "anpr-events";
    public static final String ALERTS = "alerts";   // step 3 needs it
    private KafkaTopics() {}
}
