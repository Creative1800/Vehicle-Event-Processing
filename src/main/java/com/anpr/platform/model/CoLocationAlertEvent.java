package com.anpr.platform.model;

import java.time.Instant;
import java.util.*;

/**
 * A CO_LOCATION finding on its way out of the Flink job towards the "alerts" topic.
 * CoLocationAlert is the same finding as the prototype models it.
 *
 * A POJO rather than a record, for the reason AnprEvent explains. The cost is that this
 * cannot be immutable the way CoLocationAlert is: no final fields, and the sets are
 * stored as handed over rather than defensively copied, because Flink's serializer builds
 * the object empty and fills the fields in afterwards.
 */
public class CoLocationAlertEvent {

    /**
     * Separates the parts of dedupeKey(). A pipe rather than a dash because location ids
     * contain dashes - LOC-D1-E12 - and a separator that occurs inside the values it
     * separates cannot be unambiguously read back.
     */
    private static final String DELIMITER = "|";

    /** Discriminator on the "alerts" topic, which carries SINGLE_MATCH as well. */
    public static final String TYPE = "CO_LOCATION";

    public String locationId;
    public Set<String> plates;
    public Set<String> cameraIds;

    /** Window bounds as epoch millis - when the vehicles were there, not when we noticed. */
    public long windowStartMillis;
    public long windowEndMillis;

    /** Required by Flink's POJO rules - see AnprEvent. */
    public CoLocationAlertEvent() {
    }

    public CoLocationAlertEvent(String locationId, Set<String> plates, Set<String> cameraIds,
                                long windowStartMillis, long windowEndMillis) {
        this.locationId = locationId;
        this.plates = plates;
        this.cameraIds = cameraIds;
        this.windowStartMillis = windowStartMillis;
        this.windowEndMillis = windowEndMillis;
    }

    /**
     * A getter rather than a field: Jackson serializes it, so "type" reaches the topic,
     * but Flink sees no extra field to carry through state and every shuffle. The value is
     * constant - the class IS the type.
     */
    public String getType() {
        return TYPE;
    }

    /**
     * Identifies the incident, not the window: every overlapping window that sees the same
     * plates at the same location produces the same key. This is what the dedupe operator
     * keys by.
     *
     * The plates are sorted because a Set makes no promise about iteration order, least of
     * all across a serialization round trip. Sorting makes the key depend on WHICH plates
     * were seen and never on the order they arrived in.
     */
    public String dedupeKey() {
        List<String> sorted = new ArrayList<>(plates);
        Collections.sort(sorted);

        return locationId + DELIMITER + String.join(DELIMITER, sorted);
    }

    @Override
    public String toString() {
        return "ALERT  CO_LOCATION  " + new TreeSet<>(plates)
                + "  at " + locationId
                + "  seen by " + new TreeSet<>(cameraIds)
                + "  window [" + Instant.ofEpochMilli(windowStartMillis)
                + " .. " + Instant.ofEpochMilli(windowEndMillis) + ")";
    }
}
