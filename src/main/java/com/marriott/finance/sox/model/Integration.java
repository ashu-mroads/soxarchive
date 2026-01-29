package com.marriott.finance.sox.model;

import java.util.Objects;

public final class Integration {
    private final String icNumber;
    private final String source;
    private final String destination;
    private final String id;

    public Integration(String id, String source, String destination) {
        this.icNumber = id;
        this.source = source;
        this.destination = destination;
        
        if (destination =="N/A") {
        	destination = "NA";
        }
        this.id = String.join("-",
                Objects.toString(this.icNumber, ""),
                Objects.toString(this.source, ""),
                Objects.toString(destination, ""));
    }

    public String getIcNumber() { return icNumber; }
    public String getSource() { return source; }
    public String getDestination() { return destination; }
    public String getId() { return id; }

    @Override
    public String toString() {
        return "Integration{id='" + id + "', source='" + source + "', destination='" + destination + "'}";
    }
}
