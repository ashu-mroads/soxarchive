// java
package com.marriott.finance.soxarchive.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marriott.finance.soxarchive.config.AppConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public final class S3Verify {

    private static final String ARCHIVE_SESSION_NAME = "sox-archive-session";
    private static final String CHECKPOINT_SESSION_NAME = "sox-checkpoint-session";
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    private final AppConfig config;

    public S3Verify(AppConfig config) {
        this.config = config;
    }

    public void runVerification() throws Exception {
        // Build clients (archive and checkpoint may use different assume-role ARNs)
        S3Client archiveClient = buildS3Client(true);
        S3Client checkpointClient = buildS3Client(false);

        // 1) create & upload test archive
        File zip = createTestZip();
        String archiveKey = uploadTestArchive(archiveClient, config.getS3DataBucketName(), zip);
        System.out.println("Uploaded test archive: " + archiveKey);

        // 2) list most recent 10 files in archive prefix
        System.out.println("Recent archives:");
        listRecentKeys(archiveClient, config.getS3DataBucketName(), "bizevents/", 10)
                .forEach(k -> System.out.println("  " + k));

        // 3) create a test checkpoint file
        String integrationId = "verify-integration";
        uploadTestCheckpoint(checkpointClient, config.getS3CheckpointBucketName(), integrationId);
        System.out.println("Uploaded test checkpoint for integration=" + integrationId);

        // 4) list most recent 10 checkpoints
        System.out.println("Recent checkpoints:");
        listRecentKeys(checkpointClient, config.getS3CheckpointBucketName(), "checkpoints/", 10)
                .forEach(k -> System.out.println("  " + k));

        // cleanup temp zip
        zip.delete();
        archiveClient.close();
        checkpointClient.close();
    }

    private S3Client buildS3Client(boolean forArchive) {
        Region region = Region.of(config.awsRegion());
        S3ClientBuilder builder = S3Client.builder().region(region);

        AwsCredentialsProvider baseCredentials;
        if (config.useLocalstack()) {
            baseCredentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
        } else {
            baseCredentials = DefaultCredentialsProvider.create();
        }

        AwsCredentialsProvider effectiveCredentials = baseCredentials;
        String roleArn = forArchive ? config.assumeRolearchiveArn() : config.assumeRoleCheckpointArn();
        String sessionName = forArchive ? ARCHIVE_SESSION_NAME : CHECKPOINT_SESSION_NAME;

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
                    .roleSessionName(sessionName)
                    .build();

            effectiveCredentials = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(assumeRequest)
                    .build();
        }

        if (config.useLocalstack() || (config.s3Endpoint() != null && !config.s3Endpoint().isEmpty())) {
            builder = builder
                    .endpointOverride(URI.create(config.s3Endpoint()))
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                    .credentialsProvider(effectiveCredentials);
        } else {
            builder = builder.credentialsProvider(effectiveCredentials);
        }

        return builder.build();
    }

    private File createTestZip() throws Exception {
        String fileName = "sox-verify-" + DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(ZonedDateTime.now(ZoneOffset.UTC)) + ".zip";
        File tmp = new File(config.getTempLocalDir(), fileName);
        tmp.getParentFile().mkdirs();

        try (FileOutputStream fos = new FileOutputStream(tmp);
             ZipOutputStream zos = new ZipOutputStream(fos)) {

            ZipEntry entry = new ZipEntry("verify.txt");
            zos.putNextEntry(entry);
            String content = "sox verify " + Instant.now().toString();
            zos.write(content.getBytes("UTF-8"));
            zos.closeEntry();
        }
        return tmp;
    }

    private String uploadTestArchive(S3Client s3, String bucket, File zipFile) {
        Instant now = Instant.now();
        ZonedDateTime zdt = now.atZone(ZoneOffset.UTC);
        String key = "bizevents/"
                + "integration=verify-integration/"
                + "year=" + zdt.getYear() + "/"
                + "month=" + pad(zdt.getMonthValue()) + "/"
                + "day=" + pad(zdt.getDayOfMonth()) + "/"
                + "hour=" + pad(zdt.getHour()) + "/"
                + zipFile.getName();

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/zip")
                .build();

        s3.putObject(req, RequestBody.fromFile(zipFile));
        return key;
    }

    private void uploadTestCheckpoint(S3Client s3, String bucket, String integrationId) throws Exception {
        String key = "checkpoints/integration=" + integrationId + "/checkpoint.json";

        Map<String, Object> payload = new HashMap<>();
        payload.put("integrationId", integrationId);
        payload.put("lastProcessedTimestamp", Instant.now().toString());
        payload.put("updatedAt", Instant.now().toString());

        byte[] bytes = MAPPER.writeValueAsBytes(payload);

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/json")
                .build();

        s3.putObject(req, RequestBody.fromBytes(bytes));
    }

    private List<String> listRecentKeys(S3Client s3, String bucket, String prefix, int limit) {
        ListObjectsV2Request req = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxKeys(1000)
                .build();

        ListObjectsV2Response resp = s3.listObjectsV2(req);
        List<S3Object> contents = resp.contents();

        return contents.stream()
                .sorted(Comparator.comparing(S3Object::lastModified).reversed())
                .limit(limit)
                .map(o -> o.key() + " (lastModified=" + o.lastModified() + ", size=" + o.size() + ")")
                .collect(Collectors.toList());
    }

    private static String pad(int v) {
        return v < 10 ? "0" + v : String.valueOf(v);
    }
}
