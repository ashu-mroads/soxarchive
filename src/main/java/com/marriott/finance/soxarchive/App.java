// java
package com.marriott.finance.soxarchive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.marriott.finance.soxarchive.auth.DynatraceOAuthClient;
import com.marriott.finance.soxarchive.config.AppConfig;
import com.marriott.finance.soxarchive.config.EnvConfigLoader;
import com.marriott.finance.soxarchive.model.Integration;
import com.marriott.finance.soxarchive.model.Integrations;
import com.marriott.finance.soxarchive.s3.S3CheckpointStore;
import com.marriott.finance.soxarchive.s3.S3Uploader;
import com.marriott.finance.soxarchive.s3.S3Verify;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static final int MAX_PARALLEL_EXECUTIONS = 4;

    public static void main(String[] args) {
        log.info("Starting Dynatrace Bizevents Exporter");

        try {
            AppConfig config = EnvConfigLoader.load();
            DynatraceOAuthClient oauthClient =
                    new DynatraceOAuthClient(
                            config.oauthTokenUrl(),
                            config.oauthClientId(),
                            config.oauthClientSecret(),
                            config.oauthScope(),
                            config.oauthResourceURN()
                    );

            String configStr;
            try {
                ObjectMapper mapper = new ObjectMapper()
                        .findAndRegisterModules()
                        .enable(SerializationFeature.INDENT_OUTPUT);
                configStr = mapper.writeValueAsString(config);
            } catch (Exception e) {
                configStr = String.valueOf(config);
            }

            log.info("Loaded configuration: {}", configStr);
            //call verify
            
            S3Verify s3Verify = new S3Verify(config);
            s3Verify.runVerification();

            List<Integration> integrations = Integrations.getAllIntegrations();
            if (integrations == null || integrations.isEmpty()) {
                log.info("No integrations to process");
                System.exit(0);
            }
            if (integrations.size() == 1) {
				log.info("Found 1 integration to process");
			} else {
				log.info("Found {} integrations to process", integrations.size());
			}

            int available = Runtime.getRuntime().availableProcessors();
            int poolSize = Math.max(1, Math.min(Math.min(MAX_PARALLEL_EXECUTIONS, integrations.size()), available));
            log.info("Launching {} integration worker(s) (available CPUs={}, configured max={})", poolSize, available, MAX_PARALLEL_EXECUTIONS);

            ExecutorService executor = Executors.newFixedThreadPool(poolSize);
            List<Future<?>> futures = new ArrayList<>(integrations.size());
            AtomicBoolean hadFailure = new AtomicBoolean(false);

            S3Uploader s3Uploader = new S3Uploader(config);
            S3CheckpointStore s3CheckpointStore = new S3CheckpointStore(config);

            for (Integration integration : integrations) {              
                BizeventsClient bizeventsClient = new BizeventsClient(config, oauthClient);
                futures.add(
                        executor.submit(() -> {
                            try {
                                ProcessIntegration.processIntegration(bizeventsClient, s3CheckpointStore, s3Uploader, integration, config);
                            } catch (Exception e) {
                                hadFailure.set(true);
                                log.error("[{}] Integration task failed", integration.getId(), e);
                            }
                        })
                );
            }

            executor.shutdown();

            waitForTasksToFinish(config, executor);

            // check futures for exceptions that might have been thrown
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    hadFailure.set(true);
                    log.debug("Future completed exceptionally", e);
                }
            }

            // give extra time for logs upload
            try {
                log.info("Waiting {} seconds to allow logs to be uploaded to Dynatrace", config.getTimeWaitAfterUploadSecs());
                TimeUnit.SECONDS.sleep(config.getTimeWaitAfterUploadSecs());
            } catch (InterruptedException ie) {
                log.warn("Interrupted while waiting for logs upload", ie);
                Thread.currentThread().interrupt();
            }

            if (hadFailure.get()) {
                log.error("One or more integration tasks failed");
                System.exit(1);
            } else {
                log.info("All integration tasks completed successfully");
                System.exit(0);
            }

        } catch (Exception e) {
            log.error("Job failed", e);
            System.exit(1);
        }
    }

    private static void waitForTasksToFinish(AppConfig config, ExecutorService executor) {
        long totalWaitSecs = TimeUnit.HOURS.toSeconds(config.getMaxTaskDurationHours());
        long pollIntervalSecs = TimeUnit.MINUTES.toSeconds(5);
        long waitedSecs = 0L;
        boolean finished = false;

        while (waitedSecs < totalWaitSecs) {
            long remaining = totalWaitSecs - waitedSecs;
            long waitFor = Math.min(pollIntervalSecs, remaining);
            log.info("Waiting up to {} seconds for integration tasks to finish (total waited {}/{} sec)", waitFor, waitedSecs, totalWaitSecs);
            try {
                finished = executor.awaitTermination(waitFor, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                log.warn("Interrupted while awaiting termination; preserving interrupt status and breaking wait loop", ie);
                Thread.currentThread().interrupt();
                break;
            }
            waitedSecs += waitFor;
            log.info("awaitTermination returned {} after waiting {} seconds (total waited {}/{})", finished, waitFor, waitedSecs, totalWaitSecs);
            if (finished) {
                break;
            }
        }

        if (!finished) {
            log.warn("Timeout waiting for integration tasks to finish after {} seconds, attempting shutdownNow", totalWaitSecs);
            List<Runnable> dropped = executor.shutdownNow();
            log.warn("shutdownNow returned {} pending tasks", dropped.size());
            try {
                boolean afterGrace = executor.awaitTermination(config.getTimeWaitAfterUploadSecs(), TimeUnit.SECONDS);
                log.info("awaitTermination after shutdownNow returned {}", afterGrace);
            } catch (InterruptedException ie) {
                log.warn("Interrupted during final awaitTermination after shutdownNow", ie);
                Thread.currentThread().interrupt();
            }
        }
    }
}
