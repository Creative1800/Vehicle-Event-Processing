package com.anpr.platform.correlate;

import com.anpr.platform.data.CameraRegistry;
import com.anpr.platform.data.Watchlist;
import com.anpr.platform.model.CoLocationAlert;
import com.anpr.platform.model.Detection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rules for CO_LOCATION, written as assertions rather than prose.
 *
 * This is the contract the Flink job will have to satisfy too. The implementation is
 * going to change completely - HashMap state becomes Flink keyed state, the manual age
 * check becomes a sliding event-time window - so these tests are what will tell us the
 * REWRITE still means the same thing.
 *
 * Every test builds its world in memory. No CSV files, no working directory, no order
 * dependence between tests.
 */
class CoLocationDetectorTest {

    private static final Instant BASE = Instant.parse("2026-09-18T08:00:00Z");

    private static final String PLATE_A = "AA111AA";
    private static final String PLATE_B = "BB222BB";
    private static final String PLATE_C = "CC333CC";
    private static final String PLATE_UNLISTED = "ZZ999ZZ";

    private static final Watchlist WATCHLIST = Watchlist.of(PLATE_A, PLATE_B, PLATE_C);

    /** CAM-01 and CAM-04 deliberately share a location; CAM-02 is somewhere else. */
    private static final CameraRegistry CAMERAS = CameraRegistry.of(
            Map.of("CAM-01", "LOC-RING",
                    "CAM-04", "LOC-RING",
                    "CAM-02", "LOC-EXIT"),
            Map.of("LOC-RING", "Ring Road",
                    "LOC-EXIT", "Exit 12"));

    private CoLocationDetector detector;

    /**
     * A detector holds state and never evicts it, so a shared instance would leak one
     * test's sightings into the next. Each test gets its own.
     */
    @BeforeEach
    void freshDetector() {
        detector = new CoLocationDetector(WATCHLIST, CAMERAS);
    }

    /** Keeps the tests readable: only plate, camera and relative time ever matter here. */
    private static Detection detection(String plate, String cameraId, long minutesAfterBase) {
        return new Detection(
                "DET-" + plate + "-" + minutesAfterBase,
                plate,
                cameraId,
                BASE.plus(Duration.ofMinutes(minutesAfterBase)),
                0.95);
    }

    @Test
    void twoDifferentWatchlistedPlatesInsideTheWindowRaiseOneAlert() {
        List<CoLocationAlert> alerts = detector.detectAll(List.of(
                detection(PLATE_A, "CAM-02", 0),
                detection(PLATE_B, "CAM-02", 5)));

        assertEquals(1, alerts.size());

        CoLocationAlert alert = alerts.get(0);
        assertEquals(Set.of(PLATE_A, PLATE_B), alert.plates());
        assertEquals("LOC-EXIT", alert.locationId());
        assertEquals(Set.of("CAM-02"), alert.cameraIds());
        assertEquals(BASE.plus(Duration.ofMinutes(5)), alert.asOf(),
                "the alert is stamped with the detection that completed the group");
    }

    @Test
    void theSamePlateSeenTwiceIsStillOneVehicleAndDoesNotAlert() {
        List<CoLocationAlert> alerts = detector.detectAll(List.of(
                detection(PLATE_A, "CAM-01", 0),
                detection(PLATE_A, "CAM-04", 5)));

        assertTrue(alerts.isEmpty(),
                "one vehicle crossing two cameras of one junction is not a co-location");
    }

    @Test
    void twoPlatesFurtherApartThanTheWindowDoNotAlert() {
        List<CoLocationAlert> alerts = detector.detectAll(List.of(
                detection(PLATE_A, "CAM-01", 0),
                detection(PLATE_B, "CAM-01", 20)));

        assertTrue(alerts.isEmpty(), "20 minutes apart is outside the default 15 minute window");
    }

    @Test
    void platesSeenByDifferentCamerasAtOneLocationAlertAndNameBothCameras() {
        List<CoLocationAlert> alerts = detector.detectAll(List.of(
                detection(PLATE_A, "CAM-01", 0),
                detection(PLATE_B, "CAM-04", 6)));

        assertEquals(1, alerts.size());

        CoLocationAlert alert = alerts.get(0);
        assertEquals("LOC-RING", alert.locationId());
        assertEquals(Set.of("CAM-01", "CAM-04"), alert.cameraIds(),
                "co-location is keyed by location, so both cameras contribute to one alert");
    }

    @Test
    void platesAtDifferentLocationsDoNotCorrelate() {
        List<CoLocationAlert> alerts = detector.detectAll(List.of(
                detection(PLATE_A, "CAM-01", 0),
                detection(PLATE_B, "CAM-02", 5)));

        assertTrue(alerts.isEmpty(), "LOC-RING and LOC-EXIT are different places");
    }

    @Test
    void aPlateThatIsNotOnTheWatchlistIsIgnored() {
        List<CoLocationAlert> alerts = detector.detectAll(List.of(
                detection(PLATE_A, "CAM-01", 0),
                detection(PLATE_UNLISTED, "CAM-01", 5)));

        assertTrue(alerts.isEmpty(), "only vehicles of interest count towards a group");
    }

    @Test
    void aDetectionFromAnUnknownCameraIsSkipped() {
        List<CoLocationAlert> alerts = detector.detectAll(List.of(
                detection(PLATE_A, "CAM-01", 0),
                detection(PLATE_B, "CAM-99", 5)));

        assertTrue(alerts.isEmpty(), "an unmapped camera has no location to correlate on");
    }

    @Test
    void aGapOfExactlyTheWindowStillAlerts() {
        List<CoLocationAlert> alerts = detector.detectAll(List.of(
                detection(PLATE_A, "CAM-01", 0),
                detection(PLATE_B, "CAM-01", 15)));

        assertEquals(1, alerts.size(), "the window boundary is inclusive");
    }

    @Test
    void aShorterWindowCanBeConfigured() {
        CoLocationDetector strict = new CoLocationDetector(WATCHLIST, CAMERAS, Duration.ofMinutes(2));

        List<CoLocationAlert> alerts = strict.detectAll(List.of(
                detection(PLATE_A, "CAM-01", 0),
                detection(PLATE_B, "CAM-01", 5)));

        assertTrue(alerts.isEmpty(), "5 minutes apart is outside a 2 minute window");
    }

    @Test
    void aThirdPlateRaisesASecondOverlappingAlert() {
        List<CoLocationAlert> alerts = detector.detectAll(List.of(
                detection(PLATE_A, "CAM-01", 0),
                detection(PLATE_B, "CAM-01", 5),
                detection(PLATE_C, "CAM-01", 10)));

        assertEquals(2, alerts.size(),
                "every detection that keeps the group complete raises another alert");
        assertEquals(Set.of(PLATE_A, PLATE_B), alerts.get(0).plates());
        assertEquals(Set.of(PLATE_A, PLATE_B, PLATE_C), alerts.get(1).plates());
    }

    @Test
    void aWindowThatIsNotPositiveIsRejectedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> new CoLocationDetector(WATCHLIST, CAMERAS, Duration.ZERO));
    }

    @Test
    void aMissingWatchlistIsRejectedAtConstruction() {
        assertThrows(NullPointerException.class,
                () -> new CoLocationDetector(null, CAMERAS));
    }
}
