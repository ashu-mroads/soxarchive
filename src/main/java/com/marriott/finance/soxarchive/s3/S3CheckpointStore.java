package com.marriott.finance.soxarchive.s3;

import java.net.URI; import java.time.Instant;

import com.fasterxml.jackson.databind.ObjectMapper; import com.marriott.finance.soxarchive.config.AppConfig; import com.marriott.finance.soxarchive.model.Checkpoint; import com.marriott.finance.soxarchive.model.CheckpointStore;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials; import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider; import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider; import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider; import software.amazon.awssdk.core.ResponseInputStream; import software.amazon.awssdk.core.sync.RequestBody; import software.amazon.awssdk.regions.Region; import software.amazon.awssdk.services.s3.S3Client; import software.amazon.awssdk.services.s3.S3ClientBuilder; import software.amazon.awssdk.services.s3.S3Configuration; import software.amazon.awssdk.services.s3.model.GetObjectRequest; import software.amazon.awssdk.services.s3.model.NoSuchKeyException; import software.amazon.awssdk.services.s3.model.PutObjectRequest; import software.amazon.awssdk.services.sts.StsClient; import software.amazon.awssdk.services.sts.StsClientBuilder; import software.amazon.awssdk.services.sts.model.AssumeRoleRequest; import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

public final class S3CheckpointStore implements CheckpointStore {

	private static final String ROOT_PREFIX = "checkpoints";
	private static final String CHECKPOINT_SESSION_NAME = "sox-checkpoint-session";
	
	private final S3Client s3Client;
	private final String bucket;
	private final ObjectMapper objectMapper;

	public S3CheckpointStore(AppConfig config) {
	    this.bucket = config.getS3CheckpointBucketName();
	
	    Region region = Region.of(config.awsRegion());
	    S3ClientBuilder builder = S3Client.builder().region(region);
	
	    // Base credentials provider (either localstack static or default)
	    AwsCredentialsProvider baseCredentials;
	    if (config.useLocalstack()) {
	        baseCredentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
	    } else {
	        baseCredentials = DefaultCredentialsProvider.create();
	    }
	
	    // If assume role is configured, build an STS-based credentials provider
	    AwsCredentialsProvider effectiveCredentials = baseCredentials;
	    String roleArn = config.assumeRoleCheckpointArn(); // expects AppConfig to expose this (nullable/empty if not used)
	    if (roleArn != null && !roleArn.isEmpty()) {
	        StsClientBuilder stsBuilder = StsClient.builder().region(region);
	
	        if (config.useLocalstack()) {
	            stsBuilder = stsBuilder
	                    .endpointOverride(URI.create(config.s3Endpoint()))
	                    .credentialsProvider(baseCredentials);
	        } else if (config.s3Endpoint() != null && !config.s3Endpoint().isEmpty()) {
	            stsBuilder = stsBuilder
	                    .endpointOverride(URI.create(config.s3Endpoint()))
	                    .credentialsProvider(baseCredentials);
	        } else {
	            stsBuilder = stsBuilder.credentialsProvider(baseCredentials);
	        }
	
	        StsClient stsClient = stsBuilder.build();
	
	        AssumeRoleRequest assumeRequest = AssumeRoleRequest.builder()
	                .roleArn(roleArn)
	                .roleSessionName(CHECKPOINT_SESSION_NAME)
	                .build();
	
	        effectiveCredentials = StsAssumeRoleCredentialsProvider.builder()
	                .stsClient(stsClient)
	                .refreshRequest(assumeRequest)
	                .build();
	    }
	
	    // Configure S3 client builder (endpoint/path style and credentials)
	    if (config.useLocalstack()) {
	        builder = builder
	                .endpointOverride(URI.create(config.s3Endpoint()))
	                .serviceConfiguration(S3Configuration.builder()
	                        .pathStyleAccessEnabled(true)
	                        .build())
	                .credentialsProvider(effectiveCredentials);
	    } else if (config.s3Endpoint() != null && !config.s3Endpoint().isEmpty()) {
	        builder = builder
	                .endpointOverride(URI.create(config.s3Endpoint()))
	                .serviceConfiguration(S3Configuration.builder()
	                        .pathStyleAccessEnabled(true)
	                        .build())
	                .credentialsProvider(effectiveCredentials);
	    } else {
	        builder = builder.credentialsProvider(effectiveCredentials);
	    }
	
	    this.s3Client = builder.build();
	    this.objectMapper = new ObjectMapper()
	            .findAndRegisterModules();
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