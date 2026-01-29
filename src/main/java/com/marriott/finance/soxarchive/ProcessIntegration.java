package com.marriott.finance.soxarchive;

import com.fasterxml.jackson.databind.JsonNode;
import com.marriott.finance.soxarchive.config.AppConfig;
import com.marriott.finance.soxarchive.model.BizeventsResponse;
import com.marriott.finance.soxarchive.model.Checkpoint;
import com.marriott.finance.soxarchive.model.CheckpointStore;
import com.marriott.finance.soxarchive.model.Integration;
import com.marriott.finance.soxarchive.s3.S3Uploader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public final class ProcessIntegration {

    private static final Logger log =
            LoggerFactory.getLogger(ProcessIntegration.class);

    private static final int PAGE_SIZE = 1000;
    private static final int HOURS_PER_WINDOW = 1;
    private static final int INITIALIZE_DAYS = 1;
    private static final long MAX_ZIP_BYTES = 1L * 1024 * 1024 * 1024; // 1GB

    private ProcessIntegration() {
        // utility class
    }

    public static void processIntegration(
            BizeventsClient bizeventsClient,
            CheckpointStore checkpointStore,
            S3Uploader s3Uploader,
            Integration integration,
            AppConfig config
    ) throws Exception {

    	Instant windowStart = Instant.now()
    	        .minus(Duration.ofDays(INITIALIZE_DAYS))
    	        .truncatedTo(ChronoUnit.HOURS);
    	Instant now = Instant.now().truncatedTo(ChronoUnit.HOURS);
    	Checkpoint checkpoint = checkpointStore.load(integration.getId());
    	if (checkpoint != null) {
    	    windowStart = checkpoint.lastProcessedTimestamp().truncatedTo(ChronoUnit.HOURS);
    	}      
    	
    	
        while (windowStart.isBefore( Instant.now().truncatedTo(ChronoUnit.HOURS).minus(1, ChronoUnit.HOURS))) {

            Instant windowEnd =
                    windowStart.plus(Duration.ofHours(HOURS_PER_WINDOW));

            log.info( "[{}] Processing window {} -> {}",  integration.getId(), windowStart, windowEnd  );

            int totalCount = bizeventsClient.getCount( integration, windowStart, windowEnd );

            if (totalCount == 0) {
                log.info("[{}] No records in window", integration.getId());
                windowStart = windowEnd;
                continue;
            }

            boolean wroteData = false;

            File currentZipFile = null;
            ZipOutputStream zos = null;
            int partIndex = 1;

            try {
                // create initial zip/entry
                currentZipFile = File.createTempFile(config.getTempLocalDir() +
                        "/bizevents-" + integration.getId() + "-part" + partIndex + "-",
                        ".zip"
                );
                zos = new ZipOutputStream(new FileOutputStream(currentZipFile));
                zos.putNextEntry(new ZipEntry(integration.getId() + "_events.jsonl"));

                Instant nextPageStart = windowStart;

                while (true) {
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
                        byte[] bytes = event.toString().getBytes();
                        zos.write(bytes);
                        zos.write('\n');
                        wroteData = true;

                        zos.flush();
                        // check size and roll if exceeds limit
                        if (currentZipFile.length() >= MAX_ZIP_BYTES) {
                            // close current zip entry and stream
                            zos.closeEntry();
                            zos.close();

                            // upload rolled file if it has data
                            if (currentZipFile.length() > 0) {
                                s3Uploader.uploadZip(integration, currentZipFile, windowStart);
                                log.info("[{}] Uploaded rolled part {} ({} bytes)", integration.getId(), partIndex, currentZipFile.length());
                            }

                            Files.deleteIfExists(currentZipFile.toPath());

                            // prepare next part
                            partIndex++;
                            currentZipFile = File.createTempFile(
                                    "bizevents-" + integration.getId() + "-part" + partIndex + "-",
                                    ".zip"
                            );
                            zos = new ZipOutputStream(new FileOutputStream(currentZipFile));
                            zos.putNextEntry(new ZipEntry(integration.getId() + "_events.jsonl"));
                        }
                    }

                    nextPageStart = response.nextPageStartTime().plus(Duration.ofMillis(1));

                    totalCount = bizeventsClient.getCount( integration, windowStart, nextPageStart );
                    if(totalCount == 0) {
                        break;
                    }

                }

                // close last entry/stream for the current part
                if (zos != null) {
                    zos.closeEntry();
                    zos.close();
                    zos = null;
                }

                // upload last part if it contains data
                if (currentZipFile != null && currentZipFile.length() > 0) {
                    s3Uploader.uploadZip(integration, currentZipFile, windowStart);
                    log.info("[{}] Uploaded final part {} ({} bytes)", integration.getId(), partIndex, currentZipFile.length());
                    Files.deleteIfExists(currentZipFile.toPath());
                }

                if (wroteData) {
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

            } finally {
                // ensure streams/files cleaned up if an exception occurred
                try {
                    if (zos != null) {
                        try { zos.closeEntry(); } catch (Exception ignored) {}
                        try { zos.close(); } catch (Exception ignored) {}
                    }
                } catch (Exception ignored) {}

                if (currentZipFile != null) {
                    try {
                        Files.deleteIfExists(currentZipFile.toPath());
                    } catch (Exception ignored) {}
                }
            }

            windowStart = windowEnd;
        }
    }
}
