package com.anpr.platform.model;

import java.time.Instant;

/**
 * One ANPR camera detection.
 *
 * A record: Java generates the constructor, accessors, equals, hashCode and toString.
 * Note it carries a cameraId but no location - where that camera stands is looked up
 * separately, so a detection cannot disagree with the camera registry about it.
 */
public record Detection(
        String id,
        String plate,
        String cameraId,
        Instant timestamp,
        double confidence) {
}
