package com.anpr.platform.model;

import java.time.Instant;

/**
 * One detection as it arrives on the "anpr-events" topic, after NiFi has enriched it.
 *
 * Deliberately NOT a record, and deliberately a long instead of an Instant. Flink has to
 * serialize every element that crosses an operator boundary, and it only recognises this
 * shape - public no-arg constructor, accessible fields - as a POJO it can handle
 * efficiently. A record, or an Instant field, makes it fall back to generic Kryo
 * serialization: slower, and fragile across versions.
 *
 * locationId and watchlisted are already filled in because NiFi did those lookups
 * upstream. That is why the Flink job needs neither the camera registry nor the
 * watchlist: the correlation step only correlates.
 */
public class AnprEvent {

    public String eventId;
    public String plate;
    public String cameraId;
    public String locationId;
    public boolean isWatchlisted;

    /** Event time, as epoch millis - when the camera saw the vehicle. */
    public long eventTimeMillis;

    /** Flink requires a public no-arg constructor to treat this as a POJO. */
    public AnprEvent() {
    }

    public AnprEvent(String eventId, String plate, String cameraId, String locationId,
                     boolean isWatchlisted, long eventTimeMillis) {
        this.eventId = eventId;
        this.plate = plate;
        this.cameraId = cameraId;
        this.locationId = locationId;
        this.isWatchlisted = isWatchlisted;
        this.eventTimeMillis = eventTimeMillis;
    }

    @Override
    public String toString() {
        return plate + "@" + cameraId + "/" + locationId
                + " " + Instant.ofEpochMilli(eventTimeMillis)
                + (isWatchlisted ? " [watchlisted]" : "");
    }
}
