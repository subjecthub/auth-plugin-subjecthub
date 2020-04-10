package com.migratorydata.extensions.user;

import java.util.HashMap;
import java.util.Map;

public class User {

    private String subjecthubId;

    private int maxPublishMessagesPerHour = 100;
    private int maxConnections = 100;

    private int currentConnections = 0;
    private Map<String, Integer> serverCurrentConnections = new HashMap<>();

    public User(String subjecthubId) {
        this.subjecthubId = subjecthubId;
    }

    public void updateMaxLimits(int maxConnections, int maxPublishMessagesPerHour) {
        this.maxConnections = maxConnections;
        this.maxPublishMessagesPerHour = maxPublishMessagesPerHour;
    }

    public void countClients(String serverName, Integer newConnections) {
        Integer currentConnections = serverCurrentConnections.get(serverName);
        if (currentConnections != null) {
            this.currentConnections -= currentConnections;
        }

        this.currentConnections += newConnections;

        serverCurrentConnections.put(serverName, Integer.valueOf(newConnections.intValue()));
    }

    public String getSubjecthubId() {
        return subjecthubId;
    }

    public int getConnectionsCount() {
        return currentConnections;
    }

    public boolean isConnectionsLimitExceeded() {
        return currentConnections >= maxConnections;
    }
}
