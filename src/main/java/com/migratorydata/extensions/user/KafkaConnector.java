package com.migratorydata.extensions.user;

import java.util.Objects;

public class KafkaConnector {

    private String subjecthubId;
    private String configuration;
    private String endpoint;
    private String migratoryDataSubject;

    private String configurationSubject;

    public KafkaConnector(String subjecthubId, String configuration, String endpoint, String migratoryDataSubject) {
        this.subjecthubId = subjecthubId;
        this.configuration = configuration;
        this.endpoint = endpoint;
        this.migratoryDataSubject = "/" + subjecthubId + "/" + migratoryDataSubject;

        configurationSubject = "/" + subjecthubId + "/" + configuration;
    }

    public String getSubjecthubId() {
        return subjecthubId;
    }

    public String getConfiguration() {
        return configuration;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getMigratoryDataSubject() {
        return migratoryDataSubject;
    }

    public String getConfigurationSubject() {
        return configurationSubject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaConnector source = (KafkaConnector) o;
        return Objects.equals(subjecthubId, source.subjecthubId) &&
                Objects.equals(configuration, source.configuration) &&
                Objects.equals(endpoint, source.endpoint) &&
                Objects.equals(migratoryDataSubject, source.migratoryDataSubject) &&
                Objects.equals(configurationSubject, source.configurationSubject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subjecthubId, configuration, endpoint, migratoryDataSubject, configurationSubject);
    }

    @Override
    public String toString() {
        return "Source{" +
                "subjecthubId='" + subjecthubId + '\'' +
                ", configuration='" + configuration + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", migratoryDataSubject='" + migratoryDataSubject + '\'' +
                ", configurationSubject='" + configurationSubject + '\'' +
                '}';
    }
}
