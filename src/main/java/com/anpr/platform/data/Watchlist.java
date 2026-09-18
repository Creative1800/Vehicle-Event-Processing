package com.anpr.platform.data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The vehicles of interest.
 *
 * The one place that knows where the watchlist comes from. Callers only ever ask
 * "is this plate on it?", so moving the watchlist into a database later means
 * rewriting loadFrom and nothing else.
 */
public final class Watchlist {

    private final Set<String> plates;

    private Watchlist(Set<String> plates) {
        this.plates = plates;
    }

    public static Watchlist loadFrom(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);

        Set<String> plates = new HashSet<>();
        for (int i = 1; i < lines.size(); i++) {
            plates.add(lines.get(i).split(",")[0]);
        }

        return new Watchlist(plates);
    }

    public boolean contains(String plate) {
        return plates.contains(plate);
    }

    public int size() {
        return plates.size();
    }
}
