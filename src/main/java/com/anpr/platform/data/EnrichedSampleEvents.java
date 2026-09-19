package com.anpr.platform.data;

import com.anpr.platform.model.AnprEvent;
import com.anpr.platform.model.Detection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The sample detections, enriched - what the one-shot publisher sends.
 *
 * Enrichment itself lives in Enricher, shared with the simulator, so the two feeds cannot
 * drift into disagreeing about what an enriched event is.
 */
public final class EnrichedSampleEvents {

    public static List<AnprEvent> enrichedSampleEvents() throws IOException {
        Enricher enricher = Enricher.fromSampleData();

        List<AnprEvent> events = new ArrayList<>();

        for (Detection detection : DetectionReader.readAll(SampleData.DETECTIONS)) {
            AnprEvent event = enricher.enrich(detection);
            if (event != null) {
                events.add(event);
            }
        }

        return events;
    }

    private EnrichedSampleEvents() {
    }
}
