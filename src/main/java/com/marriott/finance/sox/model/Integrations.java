package com.marriott.finance.sox.model;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Integrations {

    private Integrations() {}

    private static final List<Integration> ALL_INTEGRATIONS;

    static {
        List<Integration> list = new ArrayList<>();

        // SingleIntegrations (skip chunkSize)
        list.add(new Integration("IC-07", "INT08-1", "N/A"));
        list.add(new Integration("IC-08", "INT09-1", "N/A"));
        list.add(new Integration("IC-09", "INT10-1", "N/A"));

        // IntegrationPairs (skip chunkSize)
        list.add(new Integration("IC-01", "INT03-1", "INT04"));
        list.add(new Integration("IC-02", "INT03-2", "INT04"));
        list.add(new Integration("IC-03", "INT04", "INT31"));
        list.add(new Integration("IC-04", "INT11-2", "INT11"));
        list.add(new Integration("IC-05", "INT12-2", "INT12-1"));
        list.add(new Integration("IC-06", "INT04", "INT15-1-1"));
        list.add(new Integration("IC-10", "INT15-2-2", "INT15-2-1"));
        list.add(new Integration("IC-11", "INT15-3-2", "INT15-3-1"));
        list.add(new Integration("IC-12", "INT27", "INT28"));
        list.add(new Integration("IC-13", "INT17", "INT18"));
        list.add(new Integration("IC-14", "INT28", "INT29"));
        list.add(new Integration("IC-15", "INT25", "INT26"));
        list.add(new Integration("IC-16", "INT26", "INT30"));
        list.add(new Integration("IC-17", "INT32-2", "INT32-1"));
        list.add(new Integration("IC-18", "INT33-2", "INT33-1"));
        list.add(new Integration("IC-19", "INT15-2-2", "INT24-1"));
        list.add(new Integration("IC-20", "INT21", "INT22"));
        list.add(new Integration("IC-24", "INT16", "INT17"));
        list.add(new Integration("IC-25", "INT20", "INT16"));
        list.add(new Integration("IC-26", "INT15-1-1", "INT19-1"));
        list.add(new Integration("IC-27", "INT15-2-1", "INT19-2"));
        list.add(new Integration("IC-28", "INT15-3-1", "INT19-3"));
        list.add(new Integration("IC-29", "INT19-1", "INT20"));
        list.add(new Integration("IC-30", "INT19-2", "INT20"));

        ALL_INTEGRATIONS = Collections.unmodifiableList(list);
    }

    public static List<Integration> getAllIntegrations() {
        return ALL_INTEGRATIONS;
    }

    public static Integration findById(String id) {
        if (id == null) return null;
        for (Integration i : ALL_INTEGRATIONS) {
            if (id.equalsIgnoreCase(i.getIcNumber())) {
                return i;
            }
        }
        return null;
    }
}