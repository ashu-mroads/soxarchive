
package com.marriott.finance.sox.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import com.marriott.finance.sox.model.Checkpoint;
import com.marriott.finance.sox.model.CheckpointStore;


import java.time.Instant;

/**
 * S3-backed checkpoint store.
 *
 * Path format:
 *   checkpoints/integration=<integration-id>/checkpoint.json
 */
public final class S3CheckpointStore implements CheckpointStore {

    private static final String ROOT_PREFIX = "checkpoints";

    private final S3Client s3Client;
    private final ObjectMapper objectMapper;
    private final String bucket;

    public S3CheckpointStore(String bucket) {
        this.s3Client = S3Client.builder().build();
        this.objectMapper = new ObjectMapper();
        this.bucket = bucket;
    }

    @Override
    public Checkpoint load(String integrationId) {

        String key = checkpointKey(integrationId);

        try (ResponseInputStream<?> in =
                     s3Client.getObject(
                             GetObjectRequest.builder()
                                     .bucket(bucket)
                                     .key(key)
                                     .build()
                     )) {

            return objectMapper.readValue(in, Checkpoint.class);

        } catch (NoSuchKeyException e) {
            return Checkpoint.initial(integrationId);

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load checkpoint for integration "
                            + integrationId,
                    e
            );
        }
    }

    @Override
    public void save(Checkpoint checkpoint) {

        String key = checkpointKey(checkpoint.integrationId());

        try {
            Checkpoint updated =
                    new Checkpoint(
                            checkpoint.integrationId(),
                            checkpoint.lastProcessedTimestamp(),
                            Instant.now()
                    );

            byte[] payload =
                    objectMapper.writeValueAsBytes(updated);

            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromBytes(payload)
            );

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to save checkpoint for integration "
                            + checkpoint.integrationId(),
                    e
            );
        }
    }

    private String checkpointKey(String integrationId) {
        return ROOT_PREFIX
                + "/integration=" + integrationId
                + "/checkpoint.json";
    }
}
