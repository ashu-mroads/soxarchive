package com.marriott.finance.soxarchive.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public final class DynatraceOAuthClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    private final String tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final String resourceUrn;

    private String accessToken;
    private Instant expiresAt;

    public DynatraceOAuthClient(
            String tokenUrl,
            String clientId,
            String clientSecret,
            String scope,
            String resourceUrn
    ) {
        this.httpClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.objectMapper = new ObjectMapper();
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.resourceUrn = resourceUrn;
    }

    /**
     * Returns a valid access token, refreshing if needed.
     */
    public synchronized String getAccessToken() throws Exception {

        if (accessToken == null || isExpiringSoon()) {
            refreshToken();
        }

        return accessToken;
    }

    private boolean isExpiringSoon() {
        return expiresAt == null
                || Instant.now().isAfter(expiresAt.minusSeconds(60));
    }

    private void refreshToken() throws Exception {


        String body =
                "grant_type=client_credentials"
                        + "&client_id=" + encode(clientId)
                        + "&client_secret=" + encode(clientSecret)
                        + "&resource=" + encode(resourceUrn);
        
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header(
                        "Content-Type",
                        "application/x-www-form-urlencoded"
                )
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response =
                httpClient.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );

        if (response.statusCode() >= 300) {
            throw new RuntimeException(
                    "OAuth token request failed. Status="
                            + response.statusCode()
                            + ", body="
                            + response.body()
            );
        }

        JsonNode json =
                objectMapper.readTree(response.body());

        this.accessToken =
                json.path("access_token").asText();

        long expiresIn =
                json.path("expires_in").asLong(300);

        this.expiresAt =
                Instant.now().plusSeconds(expiresIn);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
