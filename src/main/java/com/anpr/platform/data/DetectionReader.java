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
 * The only Java class that reads the detections CSV, and it serves the console demo
 * only. In the pipeline, NiFi reads the CSV and Flink receives JSON through
 * AnprEventDeserializationSchema.
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
