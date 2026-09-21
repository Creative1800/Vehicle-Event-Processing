package com.anpr.platform.data;

import java.util.Map;

/**
 * Which location each camera covers.
 *
 * Several cameras can share one location - that is the whole point, and it is why
 * co-location is judged per location rather than per camera.
 */
public final class CameraRegistry {

    private final Map<String, String> cameraToLocation;

    private CameraRegistry(Map<String, String> cameraToLocation) {
        this.cameraToLocation = cameraToLocation;
    }

    /**
     * Builds a registry from a map already in memory.
     *
     * Same seam as Watchlist.of: it is what makes this class usable from a test, from a
     * Flink task, or from a database loader. Map.copyOf takes a defensive copy.
     */
    public static CameraRegistry of(Map<String, String> cameraToLocation) {
        return new CameraRegistry(Map.copyOf(cameraToLocation));
    }

    /** Returns null when the camera is not in the registry. */
    public String locationIdOf(String cameraId) {
        return cameraToLocation.get(cameraId);
    }
}
