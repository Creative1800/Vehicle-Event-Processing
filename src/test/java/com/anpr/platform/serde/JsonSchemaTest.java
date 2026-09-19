package com.anpr.platform.serde;

import com.anpr.platform.model.AnprEvent;
import com.anpr.platform.model.CoLocationAlertEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The JSON that travels on the Kafka topics, tested without Kafka.
 *
 * These schemas are the contract between NiFi and Flink. Everything else in the Kafka
 * phase needs a broker running to test; this does not, so it is worth pinning here.
 */
class JsonSchemaTest {

    private AnprEventDeserializationSchema inbound;
    private CoLocationAlertSerializationSchema outbound;

    @BeforeEach
    void setUp() throws Exception {
        // Flink calls open() before the first record. Nothing here reads the context,
        // so a null stands in for it.
        inbound = new AnprEventDeserializationSchema();
        inbound.open(null);

        outbound = new CoLocationAlertSerializationSchema();
        outbound.open(null);
    }

    @Test
    void readsTheEventNifiPublishes() throws Exception {
        String json = """
                {"eventId":"e-1","plate":"KE555ZT","cameraId":"CAM-01",
                 "locationId":"LOC-RING","watchlisted":true,"eventTimeMillis":1758182042000}
                """;

        AnprEvent event = inbound.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertEquals("e-1", event.eventId);
        assertEquals("KE555ZT", event.plate);
        assertEquals("CAM-01", event.cameraId);
        assertEquals("LOC-RING", event.locationId);
        assertTrue(event.watchlisted);
        assertEquals(1758182042000L, event.eventTimeMillis);
    }

    @Test
    void ignoresFieldsNifiAddsThatWeDoNotRead() throws Exception {
        // NiFi owns the inbound JSON and may add routing metadata at any time. That must
        // not stop the job.
        String json = """
                {"eventId":"e-1","plate":"KE555ZT","cameraId":"CAM-01","locationId":"LOC-RING",
                 "watchlisted":false,"eventTimeMillis":1,"nifiFlowFileId":"abc","confidence":0.97}
                """;

        AnprEvent event = inbound.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertEquals("e-1", event.eventId);
        assertFalse(event.watchlisted);
    }

    @Test
    void writesAnAlertAsJson() {
        CoLocationAlertEvent alert = new CoLocationAlertEvent(
                "LOC-RING",
                new LinkedHashSet<>(List.of("KE555ZT", "NIT77AB")),
                new LinkedHashSet<>(List.of("CAM-01", "CAM-04")),
                1758182700000L,
                1758183600000L);

        String json = new String(outbound.serialize(alert), StandardCharsets.UTF_8);

        assertTrue(json.contains("\"locationId\":\"LOC-RING\""), json);
        assertTrue(json.contains("KE555ZT"), json);
        assertTrue(json.contains("CAM-04"), json);
        assertTrue(json.contains("\"windowStartMillis\":1758182700000"), json);
        assertTrue(json.contains("\"windowEndMillis\":1758183600000"), json);
    }

    @Test
    void doesNotPublishTheInternalDedupeKey() {
        // dedupeKey() is how this job recognises repeats. It is not part of the alert,
        // and it must not leak onto the topic as though it were.
        CoLocationAlertEvent alert = new CoLocationAlertEvent(
                "LOC-RING",
                new LinkedHashSet<>(List.of("KE555ZT")),
                new LinkedHashSet<>(List.of("CAM-01")),
                1L, 2L);

        String json = new String(outbound.serialize(alert), StandardCharsets.UTF_8);

        assertFalse(json.contains("dedupeKey"), json);
    }

    @Test
    void survivesTheRoundTripFlinkWillDo() throws Exception {
        AnprEvent original = new AnprEvent("e-9", "TT9999OP", "CAM-02", "LOC-D1-E12", true, 42L);

        byte[] wire = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(original);
        AnprEvent back = inbound.deserialize(wire);

        assertEquals(original.eventId, back.eventId);
        assertEquals(original.plate, back.plate);
        assertEquals(original.locationId, back.locationId);
        assertEquals(original.watchlisted, back.watchlisted);
        assertEquals(original.eventTimeMillis, back.eventTimeMillis);
    }
}
