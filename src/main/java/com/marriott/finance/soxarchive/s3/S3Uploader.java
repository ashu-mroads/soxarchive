package com.marriott.finance.soxarchive.s3;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.io.File;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import com.marriott.finance.soxarchive.config.AppConfig;
import com.marriott.finance.soxarchive.model.Integration;

public final class S3Uploader {

    private final S3Client s3Client;
    private final String bucket;
    private static final String ARCHIVE_SESSION_NAME = "sox-archive-session";

    public S3Uploader(AppConfig config) {
        this.bucket = config.getS3DataBucketName();

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
        String roleArn = config.assumeRolearchiveArn(); // expects AppConfig to expose this (nullable/empty if not used)
        if (roleArn != null && !roleArn.isEmpty()) {
            StsClientBuilder stsBuilder = StsClient.builder().region(region);

            if (config.useLocalstack()) {
                // point STS to localstack endpoint and use static test creds
                stsBuilder = stsBuilder
                        .endpointOverride(URI.create(config.s3Endpoint()))
                        .credentialsProvider(baseCredentials);
            } else if (config.s3Endpoint() != null && !config.s3Endpoint().isEmpty()) {
                // if a custom endpoint is provided, use it for STS as well
                stsBuilder = stsBuilder
                        .endpointOverride(URI.create(config.s3Endpoint()))
                        .credentialsProvider(baseCredentials);
            } else {
                stsBuilder = stsBuilder.credentialsProvider(baseCredentials);
            }

            StsClient stsClient = stsBuilder.build();

            AssumeRoleRequest assumeRequest = AssumeRoleRequest.builder()
                    .roleArn(roleArn)
                    .roleSessionName( ARCHIVE_SESSION_NAME)
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
    }

    /**
     * Uploads a ZIP archive to S3 using integration + hour-based partitioning.
     */
    public void uploadZip(
            Integration integration,
            File zipFile,
            Instant timestamp
    ) {

        if (zipFile == null || !zipFile.exists()) {
            throw new IllegalArgumentException("ZIP file does not exist");
        }

        ZonedDateTime zdt = timestamp.atZone(ZoneOffset.UTC);

        String key =
                "bizevents/"
                        + "integration=" + integration.getId() + "/"
                        + "year=" + zdt.getYear() + "/"
                        + "month=" + pad(zdt.getMonthValue()) + "/"
                        + "day=" + pad(zdt.getDayOfMonth()) + "/"
                        + "hour=" + pad(zdt.getHour()) + "/"
                        + zipFile.getName();

        PutObjectRequest request =
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType("application/zip")
                        .build();

        s3Client.putObject(
                request,
                RequestBody.fromFile(zipFile)
        );
    }

    private static String pad(int value) {
        return value < 10 ? "0" + value : String.valueOf(value);
    }
}
