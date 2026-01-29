
package com.marriott.finance.soxarchive.model;

public interface CheckpointStore {

    /**
     * Load checkpoint for a given integration.
     * If none exists, return an initial checkpoint.
     */
    Checkpoint load(String integrationId);

    /**
     * Persist checkpoint for a given integration.
     */
    void save(Checkpoint checkpoint);
}
