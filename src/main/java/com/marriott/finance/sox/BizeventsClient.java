// java
package com.marriott.finance.sox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marriott.finance.sox.auth.DynatraceOAuthClient;
import com.marriott.finance.sox.config.AppConfig;
import com.marriott.finance.sox.model.BizeventsResponse;
import com.marriott.finance.sox.model.Integration;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BizeventsClient {

    private static final String DT_APP_URL = "https://%s.apps.dynatrace.com";

    private static final String EXECUTE_PATH =
            "/platform/storage/query/v1/query:execute";
    private static final String POLL_PATH =
            "/platform/storage/query/v1/query:poll";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final AppConfig config;    
    private final DynatraceOAuthClient oauthClient;
    private final String MAX_RESULT_BYTES = "100000000" ;
   
    private final int DQL_POLL_INTERVAL_MS = 1000 ;
    private final int MAX_RESULT_RECORDS = 100000 ;
    
    
    
    private static final Logger log = LoggerFactory.getLogger(BizeventsClient.class);


    public BizeventsClient(AppConfig config,  DynatraceOAuthClient oauthClient) {
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
        this.config = config;        
        this.oauthClient = oauthClient;
    }
    
    
    
    public BizeventsResponse getData( Integration integration, Instant from, Instant to, int pageSize ) throws Exception {      
		
    	String dql = buildDataDql(integration, from, to, pageSize);
		JsonNode resultNode = runDqlWithPolling(dql);
		return parseDataResult(resultNode);
	}
    
    public int getCount(Integration integration, Instant from, Instant to ) throws Exception {      
		
		String dql = buildCountDql(integration, from, to);
    	
		JsonNode resultNode = runDqlWithPolling(dql);
		
		return parseCountResult(resultNode);
    	
    }

    /**
     * Executes a DQL query and polls until completion.
     */
    public JsonNode runDqlWithPolling(String dql
    ) throws Exception {       
        
        String accessToken = oauthClient.getAccessToken(); 

        JsonNode start =
                executeQuery(dql, accessToken);

        // Immediate success
        if ("SUCCEEDED".equals(start.path("state").asText())) {
            return start;
        }

        String requestToken =
                start.path("requestToken").asText(null);

        if (requestToken == null) {
            throw new IllegalStateException(
                    "DQL did not succeed immediately and no requestToken was returned. State="
                            + start.path("state").asText()
            );
        }
        JsonNode lastPoll = start;
        for (int i = 0; i < config.maxPolls()  && "RUNNING".equals(lastPoll.path("state").asText()) ; i++) {
        	
            lastPoll = pollQuery( requestToken, config.requestTimeoutMillis(),  accessToken );     
            Thread.sleep(DQL_POLL_INTERVAL_MS);
        }

        if (!"SUCCEEDED".equals(lastPoll.path("state").asText())) {
            throw new IllegalStateException(
                    "DQL query did not succeed. Final state="
                            + lastPoll.path("state").asText()
            );
        }

        return lastPoll;
    }
    
    

    private JsonNode executeQuery(String dql, String token) throws Exception {

        
        Map<String, Object> body = Map.of(
                "query", dql,
                "maxResultBytes", MAX_RESULT_BYTES,
                "maxResultRecords", MAX_RESULT_RECORDS
        );
        
        String json = objectMapper.writeValueAsString(body);
        
        URI executeURI = URI.create(
                String.format(
                        DT_APP_URL,
                        config.tenantName()
                ) + EXECUTE_PATH
        );
        

        HttpRequest request = HttpRequest.newBuilder()
                .uri(executeURI)
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response =
                httpClient.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );
        
    	log.debug("Executed DQL query, received status: " + response.statusCode() + ", body: " + response.body());

    	

        if (response.statusCode() >= 300) {
            throw new RuntimeException(
                    "DQL execute failed. Status="
                            + response.statusCode()
                            + ", body="
                            + response.body()
            );
        }

        return objectMapper.readTree(response.body());
    }

    private JsonNode pollQuery(
            String requestToken,
            long requestTimeoutMillis,
            String accessToken
    ) throws Exception {

        String host = String.format(DT_APP_URL, config.tenantName());
        String encoded = URLEncoder.encode(requestToken, StandardCharsets.UTF_8);
        URI pollURI = URI.create(host + POLL_PATH + "?request-token=" + encoded);
        
        JsonNode result;
        JsonNode records;

        log.info("Polling DQL with requestToken: " + requestToken);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(pollURI)
                .header("Authorization", "Bearer " + accessToken)
                .header("Accept", "application/json")
                .timeout(
                        java.time.Duration.ofMillis(requestTimeoutMillis)
                )
                .GET()
                .build();

        HttpResponse<String> response =
                httpClient.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );
        
        
        result = objectMapper.readTree(response.body());
        if (response.statusCode() < 300) {
        	records = result.path("result").path("records");
        	int recordCount = records.size();
        	String query = result.path("result").path("metadata").path("grail").path("query").asText();        	
        	log.debug("Polled DQL query, received status: " + response.statusCode() + ", recordCount : " + recordCount + ", query: " + query);        	
        }

        if (response.statusCode() >= 300) {
        	log.error("DQL poll failed with status: " + response.statusCode() + ", body: " + response.body());
            throw new RuntimeException(
                    "DQL poll failed. Status="
                            + response.statusCode()
                            + ", body="
                            + response.body()
            );
        }

        return result;
    }

    public BizeventsResponse parseDataResult(JsonNode node)
            throws Exception {
    	
        JsonNode result = node.path("result");
        if (result.isMissingNode()) {
            throw new IllegalStateException(
                    "DQL SUCCEEDED but result was missing"
            );
        }
        return objectMapper.treeToValue(
                result,
                BizeventsResponse.class
        );
    }
    
    public int parseCountResult(JsonNode node)
            throws Exception {
    	
        JsonNode result = node.path("result");
        if (result.isMissingNode()) {
            throw new IllegalStateException(
                    "DQL SUCCEEDED but result was missing"
            );
        }
        
        int  recordCount = result.path("records").get(0).path("count").asInt();        
        return recordCount;
        
    }
   		

    public String buildDataDql(
            Integration integration,
            Instant from,
            Instant to,
            int pageSize
    ) {

        String dql = "fetch bizevents, bucket:{\"sox_bizevents\"}, "
                + "from: toTimestamp(\""
                + from.toString() + "\")"	
                + ", to: toTimestamp(\""
                + to.toString()+ "\")"
                + " "
                + "| filter source == \""
                + integration.getSource().toLowerCase()
                + "\"  AND destination == \""
                + integration.getDestination().toLowerCase()
                + "\" | limit " + pageSize +  " | sort timestamp asc ";
        
        log.debug("Built DQL: " + dql);        
        return dql;
    }
    
    public String buildCountDql(
            Integration integration,
            Instant from,
            Instant to
    ) {

        String dql = "fetch bizevents, bucket:{\"sox_bizevents\"}, "
                + "from: toTimestamp(\""
                + from.toString() + "\")"	
                + ", to: toTimestamp(\""
                + to.toString()+ "\")"
                + " "
                + "| filter source == \""
                + integration.getSource().toLowerCase()
                + "\"  AND destination == \""
                + integration.getDestination().toLowerCase()
                + "\" | summarize count = count()";
        
        log.debug("Built DQL: " + dql);        
        return dql;
    }

}
