import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Raises a CO_LOCATION alert when two or more DIFFERENT vehicles of interest are
 * detected at the same LOCATION within a short time of each other.
 *
 * Grouping is by location rather than by camera, so two watchlisted vehicles caught
 * by different cameras covering the same junction still count as being together.
 *
 * Run it with:   java CoLocation.java
 */
public class CoLocation {

    private static final Duration WINDOW = Duration.ofMinutes(15);

    /** One watchlisted vehicle, seen by one camera, at one moment. */
    record Sighting(String plate, String cameraId, Instant time) {}

    public static void main(String[] args) throws IOException {

        Set<String> watchlist = loadWatchlistPlates();
        Map<String, String> cameraToLocation = loadCameraLocations();
        Map<String, String> locationNames = loadLocationNames();

        // THE STATE: for each LOCATION, every watchlisted sighting we have seen there.
        Map<String, List<Sighting>> seenPerLocation = new HashMap<>();

        List<String> detections = Files.readAllLines(Path.of("sample-data", "detections.csv"));

        for (int i = 1; i < detections.size(); i++) {

            String[] columns = detections.get(i).split(",");

            String plate    = columns[1];
            String cameraId = columns[2];
            Instant time    = Instant.parse(columns[3]);

            if (!watchlist.contains(plate)) {
                continue;
            }

            String locationId = cameraToLocation.get(cameraId);
            if (locationId == null) {
                System.out.println("WARN   unknown camera " + cameraId + ", skipping " + plate);
                continue;
            }

            List<Sighting> seenHere =
                    seenPerLocation.computeIfAbsent(locationId, key -> new ArrayList<>());

            seenHere.add(new Sighting(plate, cameraId, time));

            // Of everything seen at this LOCATION, which distinct plates - and which
            // cameras - fall inside the last 15 minutes?
            Set<String> platesInWindow = new LinkedHashSet<>();
            Set<String> camerasInWindow = new LinkedHashSet<>();

            for (Sighting sighting : seenHere) {
                Duration age = Duration.between(sighting.time(), time);
                if (age.compareTo(WINDOW) <= 0) {
                    platesInWindow.add(sighting.plate());
                    camerasInWindow.add(sighting.cameraId());
                }
            }

            if (platesInWindow.size() >= 2) {
                String where = locationNames.getOrDefault(locationId, locationId);
                System.out.println("ALERT  CO_LOCATION  " + platesInWindow
                        + "  at " + where
                        + "  seen by " + camerasInWindow
                        + "  within " + WINDOW.toMinutes() + " min, as of " + time);
            }
        }
    }

    private static Set<String> loadWatchlistPlates() throws IOException {
        List<String> lines = Files.readAllLines(Path.of("sample-data", "watchlist.csv"));
        Set<String> plates = new HashSet<>();
        for (int i = 1; i < lines.size(); i++) {
            plates.add(lines.get(i).split(",")[0]);
        }
        return plates;
    }

    /** cameraId -> locationId */
    private static Map<String, String> loadCameraLocations() throws IOException {
        List<String> lines = Files.readAllLines(Path.of("sample-data", "cameras.csv"));
        Map<String, String> cameras = new HashMap<>();
        for (int i = 1; i < lines.size(); i++) {
            String[] columns = lines.get(i).split(",");
            cameras.put(columns[0], columns[1]);
        }
        return cameras;
    }

    /** locationId -> human readable name */
    private static Map<String, String> loadLocationNames() throws IOException {
        List<String> lines = Files.readAllLines(Path.of("sample-data", "locations.csv"));
        Map<String, String> names = new HashMap<>();
        for (int i = 1; i < lines.size(); i++) {
            String[] columns = lines.get(i).split(",");
            names.put(columns[0], columns[1]);
        }
        return names;
    }
}
