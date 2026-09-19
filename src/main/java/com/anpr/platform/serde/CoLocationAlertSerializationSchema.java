package com.anpr.platform.serde;

import com.anpr.platform.model.CoLocationAlertEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Writes CoLocationAlertEvent as the JSON published to "alerts".
 *
 * dedupeKey() does not appear in the output: Jackson serializes public fields and bean
 * getters, and dedupeKey() is neither. The sets serialize as JSON arrays.
 */
public class CoLocationAlertSerializationSchema
        implements SerializationSchema<CoLocationAlertEvent> {

    /** Built in open(), not the constructor - this object is shipped to the worker. */
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(CoLocationAlertEvent alert) {
        try {
            return mapper.writeValueAsBytes(alert);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize alert: " + alert, e);
        }
    }
}
