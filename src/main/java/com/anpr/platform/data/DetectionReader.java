package com.anpr.platform.data;

import com.anpr.platform.model.Detection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns the detections file into Detection objects.
 *
 * The only class that knows the events are stored as CSV. When they arrive as JSON
 * from Kafka later, this is the class that changes - nothing else.
 */
public final class DetectionReader {

    private DetectionReader() {
    }

    public static List<Detection> readAll(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);

        List<Detection> detections = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String[] columns = lines.get(i).split(",");

            detections.add(new Detection(
                    columns[0],
                    columns[1],
                    columns[2],
                    Instant.parse(columns[3]),
                    Double.parseDouble(columns[4])));
        }

        return detections;
    }
}
