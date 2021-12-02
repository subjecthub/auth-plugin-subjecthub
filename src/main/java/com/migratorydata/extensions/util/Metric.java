package com.migratorydata.extensions.util;

public class Metric {

    public final String topic;
    public final String application;
    public final int value;

    public Metric(String topic, String application, int value) {
        this.topic = topic;
        this.application = application;
        this.value = value;
    }
}
