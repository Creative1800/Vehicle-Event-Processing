import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Loads the "vehicles of interest" dataset and prints it.
 *
 * Run it with:   java Watchlist.java
 */
public class Watchlist {

    public static void main(String[] args) throws IOException {

        Path file = Path.of("sample-data", "watchlist.csv");

        List<String> lines = Files.readAllLines(file);

        System.out.println("Vehicles of interest (" + (lines.size() - 1) + "):");
        System.out.println();

        // Start at 1, not 0: line 0 is the header row (plate,reason,priority).
        for (int i = 1; i < lines.size(); i++) {

            String[] columns = lines.get(i).split(",");

            String plate    = columns[0];
            String reason   = columns[1];
            String priority = columns[2];

            System.out.println("  " + plate + "  [" + priority + "]  " + reason);
        }
    }
}
