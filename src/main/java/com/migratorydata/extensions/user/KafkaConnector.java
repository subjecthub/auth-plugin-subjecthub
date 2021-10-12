//package com.migratorydata.extensions.user;
//
//import java.util.Objects;
//
//public class KafkaConnector {
//
//    private String subjecthubId;
//    private String configuration;
//    private String endpoint;
//    private String migratoryDataSubject;
//
//    private String configurationSubject;
//
//    private String status;
//
//    public KafkaConnector(String subjecthubId, String configuration, String endpoint, String migratoryDataSubject, String status) {
//        this.subjecthubId = subjecthubId;
//        this.configuration = configuration;
//        this.endpoint = endpoint;
//        this.migratoryDataSubject = "/" + subjecthubId + "/" + migratoryDataSubject;
//
//        configurationSubject = "/" + subjecthubId + "/" + configuration;
//
//        this.status = status;
//    }
//
//    public String getSubjecthubId() {
//        return subjecthubId;
//    }
//
//    public String getConfiguration() {
//        return configuration;
//    }
//
//    public String getEndpoint() {
//        return endpoint;
//    }
//
//    public String getMigratoryDataSubject() {
//        return migratoryDataSubject;
//    }
//
//    public String getConfigurationSubject() {
//        return configurationSubject;
//    }
//
//    public String getStatus() {
//        return status;
//    }
//
//    public void setStatus(String status) {
//        this.status = status;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        KafkaConnector that = (KafkaConnector) o;
//        return Objects.equals(subjecthubId, that.subjecthubId) &&
//                Objects.equals(configuration, that.configuration) &&
//                Objects.equals(endpoint, that.endpoint) &&
//                Objects.equals(migratoryDataSubject, that.migratoryDataSubject) &&
//                Objects.equals(configurationSubject, that.configurationSubject) &&
//                Objects.equals(status, that.status);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(subjecthubId, configuration, endpoint, migratoryDataSubject, configurationSubject, status);
//    }
//
//    @Override
//    public String toString() {
//        return "KafkaConnector{" +
//                "subjecthubId='" + subjecthubId + '\'' +
//                ", configuration='" + configuration + '\'' +
//                ", endpoint='" + endpoint + '\'' +
//                ", migratoryDataSubject='" + migratoryDataSubject + '\'' +
//                ", configurationSubject='" + configurationSubject + '\'' +
//                ", status='" + status + '\'' +
//                '}';
//    }
//}
