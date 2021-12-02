package com.migratorydata.extensions.user;

import java.util.HashMap;
import java.util.Map;

public class Application {

    private Map<String, Connection> connections = new HashMap<>();

    private Key key = new Key();
    private Limit limit = new Limit(100, 5000);

    private final String topic;
    private final String application;

    private int connectionsNumber = 0;
    private int messagesNumber = 0;

    public Application(String topic, String application) {
        this.topic = topic;
        this.application = application;
    }

    public Key getKey() {
        return key;
    }

    public void addKey(String secret) {
        key.addKey(secret);
    }

    public void deleteKey(String secret) {
        key.deleteKey(secret);
    }

    public void addLimit(Limit limit) {
        this.limit = limit;
    }

    public Limit getLimit() {
        return limit;
    }

    public String getTopic() {
        return topic;
    }

    public String getApplication() {
        return application;
    }

    @Override
    public String toString() {
        return "Application [ " + application + " ] {" +
                "key=" + key +
                ", limit=" + limit +
                '}';
    }

    public void updateConnections(String serverName, Integer nrConnections) {
        Connection connection = connections.get(serverName);
        if (connection == null) {
            connection = new Connection();
            connections.put(serverName, connection);
        }
        connection.setConnections(nrConnections);

        connectionsNumber = 0;

        for (Map.Entry<String, Connection> entry : connections.entrySet()) {
            if (System.currentTimeMillis() - entry.getValue().getLastUpdate() < 16000) {
                connectionsNumber += entry.getValue().getConnections();
            }
        }
    }

    public void addMessages(int messagesNumber) {
        this.messagesNumber += messagesNumber;
    }

    public boolean isConnectionLimitExceeded() {
        if (connectionsNumber > limit.connections) {
            return true;
        }
        return false;
    }

    public boolean isMessagesLimitExceeded() {
        if (messagesNumber > limit.messages) {
            return true;
        }
        return false;
    }

    public void resetMessagesNumber() {
        messagesNumber = 0;
    }
}
