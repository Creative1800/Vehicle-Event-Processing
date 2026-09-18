package com.anpr.platform.correlate;

import com.anpr.platform.data.CameraRegistry;
import com.anpr.platform.data.Watchlist;
import com.anpr.platform.model.CoLocationAlert;
import com.anpr.platform.model.Detection;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Raises a CO_LOCATION alert when two or more DIFFERENT vehicles of interest are
 * detected at the same location within a short time of each other.
 *
 * Unlike SINGLE_MATCH this has to remember earlier detections while processing later
 * ones. That state is the reason this half of the problem belongs in Flink.
 *
 * Stateful, single threaded, and it assumes detections arrive in timestamp order. Each
 * run needs its own instance, because nothing is ever evicted from the state. Those are
 * deliberate limitations of the prototype - Flink's event time, watermarks and windows
 * are the answer to all three.
 */
public final class CoLocationDetector {

    private static final Duration DEFAULT_WINDOW = Duration.ofMinutes(15);

    /** "MULTIPLE vehicles of interest" - the business rule, as a number. */
    private static final int MIN_DISTINCT_PLATES = 2;

    /** One watchlisted vehicle, seen by one camera, at one moment. */
    private record Sighting(String plate, String cameraId, Instant time) {
    }

    private final Watchlist watchlist;
    private final CameraRegistry cameraRegistry;
    private final Duration window;

    /** THE STATE: for each location, every watchlisted sighting seen there so far. */
    private final Map<String, List<Sighting>> seenPerLocation = new HashMap<>();

    /** Correlates over the default 15 minute window. */
    public CoLocationDetector(Watchlist watchlist, CameraRegistry cameraRegistry) {
        this(watchlist, cameraRegistry, DEFAULT_WINDOW);
    }

    /** The canonical constructor: the only place fields are assigned or checked. */
    public CoLocationDetector(Watchlist watchlist, CameraRegistry cameraRegistry, Duration window) {
        Objects.requireNonNull(window, "window");
        if (window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("window must be positive, got " + window);
        }

        this.watchlist = Objects.requireNonNull(watchlist, "watchlist");
        this.cameraRegistry = Objects.requireNonNull(cameraRegistry, "cameraRegistry");
        this.window = window;
    }

    /** The window being correlated over, so callers can report it instead of guessing. */
    public Duration window() {
        return window;
    }

    /**
     * Feeds in one detection and returns an alert if this detection completed a group of
     * watchlisted vehicles at one location.
     *
     * Empty means no alert, for one of three reasons: the plate is not watchlisted, the
     * camera is unknown, or fewer than MIN_DISTINCT_PLATES distinct plates are in the
     * window. Unknown cameras are skipped silently.
     */
    public Optional<CoLocationAlert> onDetection(Detection detection) {
        if (!watchlist.contains(detection.plate())) {
            return Optional.empty();
        }

        String locationId = cameraRegistry.locationIdOf(detection.cameraId());
        if (locationId == null) {
            return Optional.empty();
        }

        List<Sighting> seenHere =
                seenPerLocation.computeIfAbsent(locationId, key -> new ArrayList<>());

        seenHere.add(new Sighting(detection.plate(), detection.cameraId(), detection.timestamp()));

        // Which distinct plates - and cameras - fall inside the window ending now?
        Set<String> platesInWindow = new LinkedHashSet<>();
        Set<String> camerasInWindow = new LinkedHashSet<>();

        for (Sighting sighting : seenHere) {
            Duration age = Duration.between(sighting.time(), detection.timestamp());

            // A negative age means the stored sighting is NEWER than the detection being
            // processed, so the input was not in timestamp order. Excluded rather than
            // counted: without this guard a sighting hours in the future passes the upper
            // bound and lands in the window.
            if (!age.isNegative() && age.compareTo(window) <= 0) {
                platesInWindow.add(sighting.plate());
                camerasInWindow.add(sighting.cameraId());
            }
        }

        if (platesInWindow.size() >= MIN_DISTINCT_PLATES) {
            return Optional.of(new CoLocationAlert(
                    platesInWindow, locationId, camerasInWindow, detection.timestamp()));
        }

        return Optional.empty();
    }

    /**
     * Convenience wrapper over onDetection for batch sources like a CSV file or a test.
     *
     * Order matters, so this loops rather than streams. It also does NOT start from a
     * clean slate: state carries over from any earlier call on this instance.
     */
    public List<CoLocationAlert> detectAll(List<Detection> detections) {
        List<CoLocationAlert> alerts = new ArrayList<>();
        for (Detection detection : detections) {
            onDetection(detection).ifPresent(alerts::add);
        }
        return alerts;
    }
}
