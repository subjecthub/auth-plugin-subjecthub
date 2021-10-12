package com.migratorydata.extensions.user;

public class Limit {

    public final int connections;
    public final int messages;

    public Limit(int connections, int messages) {
        this.connections = connections;
        this.messages = messages;
    }
}
