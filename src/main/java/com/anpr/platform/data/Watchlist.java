package com.anpr.platform.data;

import java.util.Arrays;
import java.util.Set;

/**
 * The vehicles of interest.
 *
 * The one place that knows what is on the watchlist. Callers only ever ask
 * "is this plate on it?", so a watchlist backed by a database only has to be built
 * through of() - nothing that asks the question changes.
 */
public final class Watchlist {

    private final Set<String> plates;

    private Watchlist(Set<String> plates) {
        this.plates = plates;
    }

    /**
     * Builds a watchlist from plates listed inline - the form a test wants.
     *
     * Note Set.copyOf(Arrays.asList(...)) rather than Set.of(...): Set.of throws
     * IllegalArgumentException when handed a duplicate, and a caller listing the same
     * plate twice is a harmless mistake, not something worth crashing over.
     */
    public static Watchlist of(String... plates) {
        return of(Set.copyOf(Arrays.asList(plates)));
    }

    /**
     * Builds a watchlist from a set already in memory.
     *
     * The seam that keeps this class testable, and the way a Flink job or a database
     * loader will build one without going through a local file. Set.copyOf takes a
     * defensive copy, so a watchlist cannot be changed by whoever handed the plates over.
     * Order is not preserved, and a watchlist has no meaningful order to lose.
     */
    public static Watchlist of(Set<String> plates) {
        return new Watchlist(Set.copyOf(plates));
    }

    public boolean contains(String plate) {
        return plates.contains(plate);
    }

    public int size() {
        return plates.size();
    }
}
