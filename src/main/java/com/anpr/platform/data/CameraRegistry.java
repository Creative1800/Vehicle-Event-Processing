package com.anpr.platform.data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Which location each camera covers, and what that location is called.
 *
 * Several cameras can share one location - that is the whole point, and it is why
 * co-location is judged per location rather than per camera.
 */
public final class CameraRegistry {

    private final Map<String, String> cameraToLocation;
    private final Map<String, String> locationNames;

    private CameraRegistry(Map<String, String> cameraToLocation, Map<String, String> locationNames) {
        this.cameraToLocation = cameraToLocation;
        this.locationNames = locationNames;
    }

    /**
     * Builds a registry from maps already in memory.
     *
     * Same seam as Watchlist.of: it is what makes this class usable from a test, from a
     * Flink task, or from a database loader. Map.copyOf takes defensive copies.
     */
    public static CameraRegistry of(Map<String, String> cameraToLocation,
                                    Map<String, String> locationNames) {
        return new CameraRegistry(Map.copyOf(cameraToLocation), Map.copyOf(locationNames));
    }

    /** Parses the two CSVs, then hands the maps to of(). */
    public static CameraRegistry loadFrom(Path camerasFile, Path locationsFile) throws IOException {
        return of(readKeyValue(camerasFile), readKeyValue(locationsFile));
    }

    /** Returns null when the camera is not in the registry. */
    public String locationIdOf(String cameraId) {
        return cameraToLocation.get(cameraId);
    }

    public String nameOf(String locationId) {
        if (locationId == null) {
            return "unknown location";
        }
        return locationNames.getOrDefault(locationId, locationId);
    }

    private static Map<String, String> readKeyValue(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);

        Map<String, String> map = new HashMap<>();
        for (int i = 1; i < lines.size(); i++) {
            String[] columns = lines.get(i).split(",");
            map.put(columns[0], columns[1]);
        }

        return map;
    }
}
