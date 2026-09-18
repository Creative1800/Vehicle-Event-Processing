package com.anpr.platform.app;

import com.anpr.platform.data.CameraRegistry;
import com.anpr.platform.data.DetectionReader;
import com.anpr.platform.data.SampleData;
import com.anpr.platform.data.Watchlist;
import com.anpr.platform.model.Detection;

import java.io.IOException;
import java.util.List;

/**
 * Raises a SINGLE_MATCH alert for every detection of a vehicle of interest.

 * Needs no memory of earlier detections - each one is judged on its own. That is why
 * this alert can live in NiFi and never reach Flink.
 */
public final class SingleMatchAlerts {

    public static void main(String[] args) throws IOException {

        Watchlist watchlist = Watchlist.loadFrom(SampleData.WATCHLIST);
        CameraRegistry cameras = CameraRegistry.loadFrom(SampleData.CAMERAS, SampleData.LOCATIONS);
        List<Detection> detections = DetectionReader.readAll(SampleData.DETECTIONS);

        int alertCount = 0;

        for (Detection detection : detections) {

            if (!watchlist.contains(detection.plate())) {
                continue;
            }

            String where = cameras.nameOf(cameras.locationIdOf(detection.cameraId()));

            alertCount++;
            System.out.println("ALERT  SINGLE_MATCH  " + detection.plate()
                    + "  at " + where + " [" + detection.cameraId() + "]"
                    + "  " + detection.timestamp());
        }

        System.out.println();
        System.out.println(detections.size() + " detections processed, "
                + alertCount + " alerts raised.");
    }
}
