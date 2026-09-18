package com.anpr.platform.app;

import com.anpr.platform.data.CameraRegistry;
import com.anpr.platform.data.DetectionReader;
import com.anpr.platform.data.SampleData;
import com.anpr.platform.data.Watchlist;
import com.anpr.platform.model.Detection;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Raises a CO_LOCATION alert when two or more DIFFERENT vehicles of interest are
 * detected at the same location within a short time of each other.
 *
 * Unlike SINGLE_MATCH this has to remember earlier detections while processing later
 * ones. That state is the reason this half of the problem belongs in Flink.
 */
public final class CoLocationAlerts {

    private static final Duration WINDOW = Duration.ofMinutes(15);

    /** One watchlisted vehicle, seen by one camera, at one moment. */
    private record Sighting(String plate, String cameraId, Instant time) {
    }

    public static void main(String[] args) throws IOException {

        Watchlist watchlist = Watchlist.loadFrom(SampleData.WATCHLIST);
        CameraRegistry cameras = CameraRegistry.loadFrom(SampleData.CAMERAS, SampleData.LOCATIONS);
        List<Detection> detections = DetectionReader.readAll(SampleData.DETECTIONS);

        // THE STATE: for each location, every watchlisted sighting seen there so far.
        Map<String, List<Sighting>> seenPerLocation = new HashMap<>();

        for (Detection detection : detections) {

            if (!watchlist.contains(detection.plate())) {
                continue;
            }

            String locationId = cameras.locationIdOf(detection.cameraId());
            if (locationId == null) {
                System.out.println("WARN   unknown camera " + detection.cameraId()
                        + ", skipping " + detection.plate());
                continue;
            }

            List<Sighting> seenHere =
                    seenPerLocation.computeIfAbsent(locationId, key -> new ArrayList<>());

            seenHere.add(new Sighting(detection.plate(), detection.cameraId(), detection.timestamp()));

            // Which distinct plates - and cameras - fall inside the last 15 minutes?
            Set<String> platesInWindow = new LinkedHashSet<>();
            Set<String> camerasInWindow = new LinkedHashSet<>();

            for (Sighting sighting : seenHere) {
                Duration age = Duration.between(sighting.time(), detection.timestamp());
                if (age.compareTo(WINDOW) <= 0) {
                    platesInWindow.add(sighting.plate());
                    camerasInWindow.add(sighting.cameraId());
                }
            }

            if (platesInWindow.size() >= 2) {
                System.out.println("ALERT  CO_LOCATION  " + platesInWindow
                        + "  at " + cameras.nameOf(locationId)
                        + "  seen by " + camerasInWindow
                        + "  within " + WINDOW.toMinutes() + " min, as of " + detection.timestamp());
            }
        }
    }
}
