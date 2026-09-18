package com.anpr.platform.app;

import com.anpr.platform.correlate.CoLocationDetector;
import com.anpr.platform.data.CameraRegistry;
import com.anpr.platform.data.DetectionReader;
import com.anpr.platform.data.SampleData;
import com.anpr.platform.data.Watchlist;
import com.anpr.platform.model.CoLocationAlert;
import com.anpr.platform.model.Detection;

import java.io.IOException;
import java.util.List;

/**
 * Runnable demo for CO_LOCATION alerts: loads the sample CSVs, feeds every detection
 * through CoLocationDetector, and prints whatever comes back.
 *
 * All the logic lives in the detector. This class only does I/O and formatting, which is
 * what makes the detector testable - and what will let a Flink job reuse it unchanged.
 */
public final class CoLocationAlerts {

    public static void main(String[] args) throws IOException {

        Watchlist watchlist = Watchlist.loadFrom(SampleData.WATCHLIST);
        CameraRegistry cameras = CameraRegistry.loadFrom(SampleData.CAMERAS, SampleData.LOCATIONS);
        List<Detection> detections = DetectionReader.readAll(SampleData.DETECTIONS);

        CoLocationDetector detector = new CoLocationDetector(watchlist, cameras);

        for (CoLocationAlert alert : detector.detectAll(detections)) {
            System.out.println("ALERT  CO_LOCATION  " + alert.plates()
                    + "  at " + cameras.nameOf(alert.locationId())
                    + "  seen by " + alert.cameraIds()
                    + "  within " + detector.window().toMinutes() + " min, as of " + alert.asOf());
        }
    }
}
