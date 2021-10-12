//package com.migratorydata.extensions.user;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class User {
//
//    private final int id;
//    private final String subjecthubId;
//
//    private int publishLimitPerHour = 100;
//    private int connectionsLimit = 100;
//
//    private int maxConcurrentUsers = 0;
//    private int numberOfNewReceivedMessage = 0;
//
//    private int currentConnections = 0;
//    private Map<String, Integer> serverCurrentConnections = new HashMap<>();
//
//    public User(int id, String subjecthubId) {
//        this.id = id;
//        this.subjecthubId = subjecthubId;
//    }
//
//    public void updateLimits(int connectionsLimit, int publishLimitPerHour) {
//        this.connectionsLimit = connectionsLimit;
//        this.publishLimitPerHour = publishLimitPerHour;
//    }
//
//    public void countConnections(String serverName, Integer newConnections) {
//        Integer currentConnections = serverCurrentConnections.get(serverName);
//        if (currentConnections != null) {
//            this.currentConnections -= currentConnections;
//        }
//
//        this.currentConnections += newConnections;
//
//        serverCurrentConnections.put(serverName, Integer.valueOf(newConnections.intValue()));
//
//        if (maxConcurrentUsers < this.currentConnections) {
//            maxConcurrentUsers = this.currentConnections;
//        }
//    }
//
//    public String getSubjecthubId() {
//        return subjecthubId;
//    }
//
//    public int getId() {
//        return id;
//    }
//
//    public boolean isConnectionsLimitExceeded() {
//        return currentConnections >= connectionsLimit;
//    }
//
//    public boolean isPublishLimitExceeded(int publishCount) {
//        return publishCount >= publishLimitPerHour;
//    }
//
//    public void addNewReceivedMessages(int newMessages) {
//        numberOfNewReceivedMessage += newMessages;
//    }
//
//    public int getMaxConcurrentUsers() {
//        return maxConcurrentUsers;
//    }
//
//    public int getAndResetNumberOfNewReceivedMessage() {
//        int newReceivedMessage = numberOfNewReceivedMessage;
//        numberOfNewReceivedMessage = 0;
//        return newReceivedMessage;
//    }
//}
