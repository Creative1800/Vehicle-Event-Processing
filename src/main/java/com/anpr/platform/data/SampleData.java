package com.anpr.platform.data;

import java.nio.file.Path;

/** Where the sample CSV files live, relative to the project root. */
public final class SampleData {

    public static final Path DETECTIONS = Path.of("sample-data", "detections.csv");
    public static final Path WATCHLIST  = Path.of("sample-data", "watchlist.csv");
    public static final Path CAMERAS    = Path.of("sample-data", "cameras.csv");
    public static final Path LOCATIONS  = Path.of("sample-data", "locations.csv");

    private SampleData() {
    }
}
