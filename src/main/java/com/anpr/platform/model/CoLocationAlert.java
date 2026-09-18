package com.anpr.platform.model;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Two or more DIFFERENT vehicles of interest seen at one location inside the same
 * time window.
 *
 * Carries a locationId rather than the location's name. The name is for humans reading
 * output, so whoever prints an alert looks it up in the CameraRegistry - that keeps
 * display text out of the alert, and later out of Kafka.
 *
 * Immutable: the compact constructor below copies the sets it is given, so an alert
 * cannot be changed from a distance once it exists.
 */
public record CoLocationAlert(
        Set<String> plates,
        String locationId,
        Set<String> cameraIds,
        Instant asOf) {

    /**
     * Compact canonical constructor. It runs before the fields are stored, so reassigning
     * a parameter here is what actually gets kept.
     *
     * LinkedHashSet preserves the order the plates were first seen, which keeps printed
     * output stable; unmodifiableSet then blocks changes through the accessor.
     */
    public CoLocationAlert {
        Objects.requireNonNull(plates, "plates");
        Objects.requireNonNull(locationId, "locationId");
        Objects.requireNonNull(cameraIds, "cameraIds");
        Objects.requireNonNull(asOf, "asOf");

        plates = Collections.unmodifiableSet(new LinkedHashSet<>(plates));
        cameraIds = Collections.unmodifiableSet(new LinkedHashSet<>(cameraIds));
    }
}
