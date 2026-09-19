package com.anpr.platform.data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The plates and cameras the simulator draws from, read straight out of the sample CSVs.
 *
 * Deliberately not built on Watchlist or CameraRegistry: those answer "is this plate on
 * the list?" and "where is this camera?", and neither can be enumerated on purpose - a
 * real watchlist is a database with a million rows. A generator needs the opposite, so it
 * gets its own reader and stays honest about being demo scaffolding.
 *
 * The watchlisted split is SCENARIO vocabulary, not domain truth: it lets the simulator
 * compose traffic worth alerting on. Whether an event is actually watchlisted is decided
 * by Enricher, looking the plate up. Composing and labelling are different jobs.
 */
public final class SampleVocabulary {

    private final List<String> watchlistedPlates;
    private final List<String> otherPlates;
    private final List<String> cameraIds;

    private SampleVocabulary(List<String> watchlistedPlates,
                             List<String> otherPlates,
                             List<String> cameraIds) {
        this.watchlistedPlates = watchlistedPlates;
        this.otherPlates = otherPlates;
        this.cameraIds = cameraIds;
    }

    public static SampleVocabulary load() throws IOException {
        Set<String> watchlisted = new HashSet<>(firstColumn(SampleData.WATCHLIST));

        // Every plate the sample detections ever saw, minus the ones on the watchlist:
        // the background traffic that proves the job's filter is doing something.
        Set<String> others = new HashSet<>();
        for (String[] row : rows(SampleData.DETECTIONS)) {
            if (!watchlisted.contains(row[1])) {
                others.add(row[1]);
            }
        }

        return new SampleVocabulary(
                List.copyOf(watchlisted), List.copyOf(others), firstColumn(SampleData.CAMERAS));
    }

    public List<String> watchlistedPlates() {
        return watchlistedPlates;
    }

    public List<String> otherPlates() {
        return otherPlates;
    }

    public List<String> cameraIds() {
        return cameraIds;
    }

    private static List<String> firstColumn(Path file) throws IOException {
        List<String> values = new ArrayList<>();
        for (String[] row : rows(file)) {
            values.add(row[0]);
        }
        return values;
    }

    /** Every line but the header, split on commas. */
    private static List<String[]> rows(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);

        List<String[]> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            rows.add(lines.get(i).split(","));
        }
        return rows;
    }
}
