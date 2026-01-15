// Java
package com.marriott.finance.sox.config;


import java.util.Map;

public final class EnvConfigLoader {

    private EnvConfigLoader() {}

    public static AppConfig load() {

        Map<String, String> env = System.getenv();
        
        String tenantName = required(env, "TENANT_NAME");

        String oauthTokenUrl = required(env, "OAUTH_TOKEN_URL");
        String oauthClientId = required(env, "OAUTH_CLIENT_ID");
        String oauthClientSecret = required(env, "OAUTH_CLIENT_SECRET");
        String oauthScope = required(env, "OAUTH_SCOPE");
        String oauthResourceURN = required(env, "OAUTH_RESOURCE_URN");

        int pageSize = integer(env, "BIZEVENTS_PAGE_SIZE", 5000);

        int maxPolls = integer(env, "MAX_POLLS", 100);
        long requestTimeoutMillis = integer(env, "REQUEST_TIMEOUT_MILLIS", 300000);

        String S3DataBucketName =
                required(env, "S3_DATA_BUCKET");
        
        String S3CheckpointBucketName = required(env, "S3_CHECKPOINT_BUCKET");        
        		

        return new AppConfig(
                tenantName,
                oauthTokenUrl,
                oauthClientId,
                oauthClientSecret,
                oauthScope,
                oauthResourceURN,
                maxPolls,
                requestTimeoutMillis,
                pageSize,
                S3DataBucketName,
                S3CheckpointBucketName
        );

    }

    private static String required(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Missing required environment variable: " + key
            );
        }
        return value;
    }

    private static int integer(
            Map<String, String> env,
            String key,
            int defaultValue
    ) {
        String value = env.get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    
}
