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
 * detected at the same camera within a short time of each other.
 *
 * Run it with:   java CoLocation.java
 */
public class CoLocation {

    private static final Duration WINDOW = Duration.ofMinutes(15);

    /** One watchlisted vehicle, seen at one camera, at one moment. */
    record Sighting(String plate, Instant time) {}

    public static void main(String[] args) throws IOException {

        Set<String> watchlist = loadWatchlistPlates();

        // THE STATE: for each camera, every watchlisted sighting we have seen there.
        // This is the thing SINGLE_MATCH never needed.
        Map<String, List<Sighting>> seenPerCamera = new HashMap<>();

        List<String> detections = Files.readAllLines(Path.of("sample-data", "detections.csv"));

        for (int i = 1; i < detections.size(); i++) {

            String[] columns = detections.get(i).split(",");

            String plate    = columns[1];
            String cameraId = columns[2];
            String location = columns[3];
            Instant time    = Instant.parse(columns[4]);

            // Vehicles that are not of interest tell us nothing here.
            if (!watchlist.contains(plate)) {
                continue;
            }

            List<Sighting> seenHere =
                    seenPerCamera.computeIfAbsent(cameraId, key -> new ArrayList<>());

            seenHere.add(new Sighting(plate, time));

            // Of everything seen at this camera, which DISTINCT plates fall inside
            // the last 15 minutes, counting back from the detection we are on?
            Set<String> platesInWindow = new LinkedHashSet<>();

            for (Sighting sighting : seenHere) {
                Duration age = Duration.between(sighting.time(), time);
                if (age.compareTo(WINDOW) <= 0) {
                    platesInWindow.add(sighting.plate());
                }
            }

            if (platesInWindow.size() >= 2) {
                System.out.println("ALERT  CO_LOCATION  " + platesInWindow
                        + "  at " + cameraId + " (" + location + ")"
                        + "  within " + WINDOW.toMinutes() + " min, as of " + time);
            }
        }
    }

    /**
     * The one place that knows where the watchlist comes from.
     */
    private static Set<String> loadWatchlistPlates() throws IOException {

        List<String> lines = Files.readAllLines(Path.of("sample-data", "watchlist.csv"));

        Set<String> plates = new HashSet<>();

        for (int i = 1; i < lines.size(); i++) {
            plates.add(lines.get(i).split(",")[0]);
        }

        return plates;
    }
}
