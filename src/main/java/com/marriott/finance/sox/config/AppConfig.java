// Java
package com.marriott.finance.sox.config;

import java.time.Duration;
import java.util.Objects;

import com.marriott.finance.sox.model.CheckpointStore;

public final class AppConfig {

    private final String tenantName;

    private final String oauthTokenUrl;
    private final String oauthClientId;
    private final String oauthClientSecret;
    private final String oauthScope;
    private final String oauthResourceURN;

    private final int maxPolls;
    private final long requestTimeoutMillis;

    private final int pageSize;
   
    private final String S3DataBucketName;
    private final String S3CheckpointBucketName;
    

    public AppConfig(
            String tenantName,
            String oauthTokenUrl,
            String oauthClientId,
            String oauthClientSecret,
            String oauthScope,
            String oauthResourceURN,
            int maxPolls,
            long requestTimeoutMillis,
            int pageSize,
            String S3DataBucketName,
            String S3CheckpointBucketName
    ) {

        this.tenantName = require(tenantName, "tenantName");

        this.oauthTokenUrl = require(oauthTokenUrl, "oauthTokenUrl");
        this.oauthClientId = require(oauthClientId, "oauthClientId");
        this.oauthClientSecret = require(oauthClientSecret, "oauthClientSecret");
        this.oauthScope = require(oauthScope, "oauthScope");
        this.oauthResourceURN = require(oauthResourceURN, "oauthResourceURN");
        this.maxPolls = maxPolls;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.pageSize = pageSize;
        this.S3DataBucketName = require(S3DataBucketName, "S3DataBucketName");
        this.S3CheckpointBucketName = require(S3CheckpointBucketName, "S3CheckpointBucketName");        
    }

    private static <T> T require(T value, String name) {
        return Objects.requireNonNull(value, name + " must not be null");
    }

    public String tenantName() {
        return tenantName;
    }



    public String oauthTokenUrl() {
        return oauthTokenUrl;
    }

    public String oauthClientId() {
        return oauthClientId;
    }

    public String oauthClientSecret() {
        return oauthClientSecret;
    }

    public String oauthScope() {
        return oauthScope;
    }

    public int maxPolls() {
        return maxPolls;
    }

    public long requestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public int pageSize() {
        return pageSize;
    }

	public String oauthResourceURN() {
		return oauthResourceURN;
	}
	public String getS3DataBucketName() {
		return S3DataBucketName;
	}
	public String getS3CheckpointBucketName() {
		return S3CheckpointBucketName;
	}
}
