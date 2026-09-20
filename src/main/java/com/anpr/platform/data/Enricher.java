package com.anpr.platform.data;

import com.anpr.platform.model.AnprEvent;
import com.anpr.platform.model.Detection;

import java.io.IOException;

/**
 * Enrichment in Java. A camera reports a plate at a camera at a time; this turns that into
 * the enriched event the "anpr-events" topic carries, by resolving where the camera stands
 * and whether the plate is wanted.
 *
 * NiFi does this for the pipeline proper - two LookupRecord stages over these same CSVs.
 * This survives because CameraSimulator publishes straight to Kafka, so the Flink job can
 * be exercised without NiFi running. The two must agree; nifi/ is the reference.
 *
 * Note what is NOT here: the confidence a detection carries does not survive, because
 * AnprEvent has no field for it. The job tolerates it as an unknown field on the wire.
 */
public final class Enricher {

    private final Watchlist watchlist;
    private final CameraRegistry cameras;

    public Enricher(Watchlist watchlist, CameraRegistry cameras) {
        this.watchlist = watchlist;
        this.cameras = cameras;
    }

    /** Loads the sample CSVs. The seam a database-backed watchlist would replace. */
    public static Enricher fromSampleData() throws IOException {
        return new Enricher(
                Watchlist.loadFrom(SampleData.WATCHLIST),
                CameraRegistry.loadFrom(SampleData.CAMERAS, SampleData.LOCATIONS));
    }

    /**
     * Returns null for a camera the registry does not know - the validation half of what
     * NiFi does, and the reason a detection cannot invent a location.
     *
     * watchlisted is looked up, never asserted by the caller. A generator that picks a
     * plate it believes is wanted still has to be right about it.
     */
    public AnprEvent enrich(Detection detection) {
        String locationId = cameras.locationIdOf(detection.cameraId());
        if (locationId == null) {
            return null;
        }

        return new AnprEvent(
                detection.id(),
                detection.plate(),
                detection.cameraId(),
                locationId,
                watchlist.contains(detection.plate()),
                detection.timestamp().toEpochMilli());
    }
}
