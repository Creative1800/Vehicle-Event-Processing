package com.anpr.platform.serde;

import com.anpr.platform.model.AnprEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Reads the JSON NiFi publishes to "anpr-events" into AnprEvent.
 *
 * AnprEvent needs no Jackson annotations: public fields and a no-arg constructor is what
 * Jackson binds to, which is the same shape Flink's POJO serializer wanted. The two
 * requirements happen to coincide.
 */
public class AnprEventDeserializationSchema implements DeserializationSchema<AnprEvent> {

    /** Built in open(), not the constructor - this object is shipped to the worker. */
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public AnprEvent deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, AnprEvent.class);
    }

    @Override
    public boolean isEndOfStream(AnprEvent nextElement) {
        return false;
    }

    /**
     * Flink cannot infer what comes out of a byte[], so the type has to be stated.
     */
    @Override
    public TypeInformation<AnprEvent> getProducedType() {
        return TypeInformation.of(AnprEvent.class);
    }
}
