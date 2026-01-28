// java
package com.marriott.finance.sox.s3;

import com.marriott.finance.sox.config.AppConfig;
import com.marriott.finance.sox.model.Integration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public final class S3Uploader {

    private final S3Client s3Client;
    private final String bucket;

    public S3Uploader(AppConfig config) {
        this.bucket = config.getS3DataBucketName();

        Region region = Region.of(config.awsRegion());
        S3ClientBuilder builder = S3Client.builder().region(region);

        if (config.useLocalstack()) {
            builder = builder
                    .endpointOverride(URI.create(config.s3Endpoint()))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build())
                    .credentialsProvider(
                            StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create("test", "test")
                            )
                    );
        } else if (config.s3Endpoint() != null && !config.s3Endpoint().isEmpty()) {
            builder = builder
                    .endpointOverride(URI.create(config.s3Endpoint()))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build())
                    .credentialsProvider(DefaultCredentialsProvider.create());
        } else {
            builder = builder.credentialsProvider(DefaultCredentialsProvider.create());
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
