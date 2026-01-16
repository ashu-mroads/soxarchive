package com.marriott.finance.sox.model;
import java.time.Duration;
import java.time.Instant;

/**
 * Represents processing state for a single integration.
 */
public record Checkpoint(
        String integrationId,
        Instant lastProcessedTimestamp,
        Instant updatedAt
) {

    /**
     * Initial checkpoint for a new integration.
     */
    public static Checkpoint initial(String integrationId) {
        return new Checkpoint(
                integrationId,
                Instant.now().minus(Duration.ofDays(1)),
                Instant.now()
        );
    }
}
