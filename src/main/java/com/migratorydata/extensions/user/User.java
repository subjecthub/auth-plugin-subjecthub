package com.migratorydata.extensions.user;

import java.util.HashMap;
import java.util.Map;

public class User {

    private String subjecthubId;

    private int publishLimitPerHour = 100;
    private int connectionsLimit = 100;

    private int currentConnections = 0;
    private Map<String, Integer> serverCurrentConnections = new HashMap<>();

    public User(String subjecthubId) {
        this.subjecthubId = subjecthubId;
    }

    public void updateLimits(int connectionsLimit, int publishLimitPerHour) {
        this.connectionsLimit = connectionsLimit;
        this.publishLimitPerHour = publishLimitPerHour;
    }

    public void countConnections(String serverName, Integer newConnections) {
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
        return currentConnections >= connectionsLimit;
    }

    public boolean isPublishLimitExceeded(int publishCount) {
        return publishCount >= publishLimitPerHour;
    }
}
