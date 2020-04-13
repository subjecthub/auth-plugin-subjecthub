package com.migratorydata.extensions.authorization.stubs;

import com.migratorydata.extensions.audit.MigratoryDataAccessListener;

import java.util.Map;

public class DisconnectEventStub implements MigratoryDataAccessListener.DisconnectEvent, MigratoryDataAccessListener.ConnectEvent {

    private String address;
    private String token;

    public DisconnectEventStub(String token, String address) {
        this.address = address;
        this.token = token;
    }

    @Override
    public String getToken() {
        return token;
    }

    @Override
    public String getClientAddress() {
        return address;
    }

    @Override
    public String getUserAgent() {
        return null;
    }

    @Override
    public String getDisconnectReason() {
        return null;
    }

    @Override
    public Map<String, Object> getAdditionalInfo() {
        return null;
    }

    @Override
    public String getTime() {
        return null;
    }
}
