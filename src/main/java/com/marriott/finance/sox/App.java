// java
package com.marriott.finance.sox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.marriott.finance.sox.auth.DynatraceOAuthClient;
import com.marriott.finance.sox.config.AppConfig;
import com.marriott.finance.sox.config.EnvConfigLoader;
import com.marriott.finance.sox.model.BizeventsResponse;
import com.marriott.finance.sox.model.Checkpoint;
import com.marriott.finance.sox.model.CheckpointStore;
import com.marriott.finance.sox.model.Integration;
import com.marriott.finance.sox.model.Integrations;
import com.marriott.finance.sox.s3.S3CheckpointStore;
import com.marriott.finance.sox.s3.S3Uploader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
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
    
    private static final int EXECUTER_TERMINATION = 15; // minutes
    
    private static final int BATCH_SIZE = 5000;
    

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

            // stringify config to JSON for clearer logging, fallback to toString()
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

            

            List<Integration> integrations = Integrations.getAllIntegrations();
            if (integrations == null || integrations.isEmpty()) {
                log.info("No integrations to process");
                System.exit(0);
            }

            int available = Runtime.getRuntime().availableProcessors();
            int poolSize = Math.max(1, Math.min(Math.min(MAX_PARALLEL_EXECUTIONS, integrations.size()), available));
            log.info("Launching {} integration worker(s) (available CPUs={}, configured max={})", poolSize, available, MAX_PARALLEL_EXECUTIONS);

            ExecutorService executor = Executors.newFixedThreadPool(poolSize);
            List<Future<?>> futures = new ArrayList<>(integrations.size());
            AtomicBoolean hadFailure = new AtomicBoolean(false);
            
                        
            S3Uploader s3Uploader = new S3Uploader(config.getS3DataBucketName());
            S3CheckpointStore s3CheckpointStore = new S3CheckpointStore(config.getS3CheckpointBucketName());
            
            for (Integration integration : integrations) {
            	BizeventsClient bizeventsClient = new BizeventsClient(config, oauthClient);
                futures.add(
                        executor.submit(() -> {
                            try {
                                ProcessIntegration.processIntegration(bizeventsClient, s3CheckpointStore, s3Uploader, integration);
                            } catch (Exception e) {
                                hadFailure.set(true);
                                log.error("[{}] Integration task failed", integration.getId(), e);
                                throw new RuntimeException(e);
                            }
                        })
                );
            }

            // no more tasks
            executor.shutdown();

            // wait for tasks with a reasonable timeout
            boolean finished = executor.awaitTermination(10, TimeUnit.MINUTES);
            if (!finished) {
                log.warn("Timeout waiting for integration tasks to finish, attempting shutdownNow");
                List<Runnable> dropped = executor.shutdownNow();
                log.warn("shutdownNow returned {} pending tasks", dropped.size());
                // give a short grace period
                executor.awaitTermination(EXECUTER_TERMINATION, TimeUnit.MINUTES);
            }

            // check futures for exceptions that might have been thrown
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    hadFailure.set(true);
                    log.debug("Future completed exceptionally", e);
                }
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



}
