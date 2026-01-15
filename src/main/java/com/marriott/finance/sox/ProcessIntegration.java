package com.marriott.finance.sox;

import com.fasterxml.jackson.databind.JsonNode;
import com.marriott.finance.sox.model.Checkpoint;
import com.marriott.finance.sox.model.CheckpointStore;
import com.marriott.finance.sox.model.BizeventsResponse;
import com.marriott.finance.sox.model.Integration;
import com.marriott.finance.sox.s3.S3Uploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public final class ProcessIntegration {

    private static final Logger log =
            LoggerFactory.getLogger(ProcessIntegration.class);

    private static final int PAGE_SIZE = 1000;
    private static final int HOURS_PER_WINDOW = 1;
    private static final int INITIALIZE_DAYS = 30;

    private ProcessIntegration() {
        // utility class
    }

    public static void processIntegration(
            BizeventsClient bizeventsClient,
            CheckpointStore checkpointStore,
            S3Uploader s3Uploader,
            Integration integration
    ) throws Exception {

    	Instant windowStart = Instant.now().minus(Duration.ofDays(INITIALIZE_DAYS));
        Instant now = Instant.now();
        Checkpoint checkpoint =
                checkpointStore.load(integration.getId());
        if (checkpoint != null) {
			windowStart = checkpoint.lastProcessedTimestamp();
		}

        while (windowStart.isBefore(now)) {

            Instant windowEnd =
                    windowStart.plus(Duration.ofHours(HOURS_PER_WINDOW));

            log.info(
                "[{}] Processing window {} -> {}",
                integration.getId(),
                windowStart,
                windowEnd
            );

            int totalCount =
                    bizeventsClient.getCount(
                            integration,
                            windowStart,
                            windowEnd
                    );

            if (totalCount == 0) {
                log.info("[{}] No records in window", integration.getId());
                windowStart = windowEnd;
                continue;
            }

            File zipFile = File.createTempFile(
                    "bizevents-" + integration.getId() + "-",
                    ".zip"
            );

            boolean wroteData = false;

            try (ZipOutputStream zos =
                         new ZipOutputStream(
                                 new FileOutputStream(zipFile))) {

                zos.putNextEntry(new ZipEntry("events.jsonl"));

                Instant nextPageStart = windowStart;

                while (true) {
                	
                	int count = bizeventsClient.getCount(integration, nextPageStart, windowEnd);
                	if (count == 0) {
						break;
					}
                	
                    BizeventsResponse response =
                            bizeventsClient.getData(
                                    integration,
                                    nextPageStart,
                                    windowEnd,
                                    PAGE_SIZE
                            );

                    if (response == null
                            || response.events() == null
                            || response.events().isEmpty()) {
                        break;
                    }

                    for (JsonNode event : response.events()) {
                        zos.write(event.toString().getBytes());
                        zos.write('\n');
                        wroteData = true;
                    }

                    nextPageStart = response.nextPageStartTime();
                    if (nextPageStart == null) {
                        break;
                    }
                }

                zos.closeEntry();
            }

            if (wroteData) {
                s3Uploader.uploadZip(
                        integration,
                        zipFile,
                        windowStart
                );

                checkpointStore.save(
                        new Checkpoint(
                                integration.getId(),
                                windowEnd,
                                Instant.now()
                        )
                );

                log.info(
                    "[{}] Window {} -> {} archived and checkpoint updated",
                    integration.getId(),
                    windowStart,
                    windowEnd
                );
            } else {
                log.info(
                    "[{}] No data written for window {} -> {}",
                    integration.getId(),
                    windowStart,
                    windowEnd
                );
            }

            Files.deleteIfExists(zipFile.toPath());
            windowStart = windowEnd;
        }
    }
}
