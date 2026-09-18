import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Raises a SINGLE_MATCH alert for every detection of a vehicle of interest.
 *
 * Run it with:   java Alerts.java
 */
public class Alerts {

    public static void main(String[] args) throws IOException {

        Set<String> watchlist = loadWatchlistPlates();

        List<String> detections = Files.readAllLines(Path.of("sample-data", "detections.csv"));

        int alertCount = 0;

        // Start at 1 to skip the header row.
        for (int i = 1; i < detections.size(); i++) {

            String[] columns = detections.get(i).split(",");

            String plate     = columns[1];
            String cameraId  = columns[2];
            String location  = columns[3];
            String timestamp = columns[4];

            if (watchlist.contains(plate)) {
                alertCount++;
                System.out.println("ALERT  SINGLE_MATCH  " + plate
                        + "  at " + cameraId + " (" + location + ")"
                        + "  " + timestamp);
            }
        }

        System.out.println();
        System.out.println((detections.size() - 1) + " detections processed, "
                + alertCount + " alerts raised.");
    }

    /**
     * The one place that knows where the watchlist comes from.
     *
     * Nothing else in the program touches the CSV - they all just ask for the set of
     * plates. Moving the watchlist into a database later means rewriting this method
     * and nothing else.
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
