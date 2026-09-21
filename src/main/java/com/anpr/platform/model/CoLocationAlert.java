package com.anpr.platform.model;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A CO_LOCATION finding as the plain-Java prototype models it: two or more DIFFERENT
 * vehicles of interest seen at one location inside the same time window.
 *
 * CoLocationAlertEvent is the same finding in the form the Flink job emits. The two exist
 * separately because this one can be a record - immutable, with its invariants checked in
 * one place - and a streaming alert cannot.
 *
 * Carries a locationId rather than the location's name. The name is for humans reading
 * output, so it is resolved by whatever displays the alert, from locations.csv - that
 * keeps display text out of the alert, out of Kafka, and out of the delivered files.
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
