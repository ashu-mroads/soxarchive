package com.marriott.finance.soxarchive.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

public record BizeventsResponse(
        List<JsonNode> events,
        Instant nextPageStartTime
) {
}
