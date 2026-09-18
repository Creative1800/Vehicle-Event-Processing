import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Raises a SINGLE_MATCH alert for every detection of a vehicle of interest.
 *
 * Run it with:   java Alerts.java
 */
public class Alerts {

    public static void main(String[] args) throws IOException {

        Set<String> watchlist = loadWatchlistPlates();

        // A detection only carries a cameraId. Where that camera actually stands is
        // reference data, looked up here - the camera itself has no idea.
        Map<String, String> cameraToLocation = loadCameraLocations();
        Map<String, String> locationNames = loadLocationNames();

        List<String> detections = Files.readAllLines(Path.of("sample-data", "detections.csv"));

        int alertCount = 0;

        // Start at 1 to skip the header row.
        for (int i = 1; i < detections.size(); i++) {

            String[] columns = detections.get(i).split(",");

            String plate     = columns[1];
            String cameraId  = columns[2];
            String timestamp = columns[3];

            if (!watchlist.contains(plate)) {
                continue;
            }

            String locationId = cameraToLocation.get(cameraId);
            String where = locationNames.getOrDefault(locationId, "unknown location");

            alertCount++;
            System.out.println("ALERT  SINGLE_MATCH  " + plate
                    + "  at " + where + " [" + cameraId + "]"
                    + "  " + timestamp);
        }

        System.out.println();
        System.out.println((detections.size() - 1) + " detections processed, "
                + alertCount + " alerts raised.");
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
