package com.anpr.platform.data;

import com.anpr.platform.model.AnprEvent;
import com.anpr.platform.model.Detection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The enrichment NiFi will do, applied to the sample CSVs: resolve the camera to a
 * location, and flag whether the plate is watchlisted.
 *
 * Lives here rather than in the Flink job because only the publisher needs it - the
 * correlation job receives events already enriched, off Kafka.
 */
public final class EnrichedSampleEvents {

    public static List<AnprEvent> enrichedSampleEvents() throws IOException {
        Watchlist watchlist = Watchlist.loadFrom(SampleData.WATCHLIST);
        CameraRegistry cameras = CameraRegistry.loadFrom(SampleData.CAMERAS, SampleData.LOCATIONS);

        List<AnprEvent> events = new ArrayList<>();

        for (Detection detection : DetectionReader.readAll(SampleData.DETECTIONS)) {
            String locationId = cameras.locationIdOf(detection.cameraId());
            if (locationId == null) {
                continue;
            }

            events.add(new AnprEvent(
                    detection.id(),
                    detection.plate(),
                    detection.cameraId(),
                    locationId,
                    watchlist.contains(detection.plate()),
                    detection.timestamp().toEpochMilli()));
        }

        return events;
    }

    private EnrichedSampleEvents() {
    }
}
