package com.migratorydata.extensions.authorization.stubs;

import com.migratorydata.extensions.audit.MigratoryDataAccessListener;

import java.util.Map;

public class ConnectEventStub implements MigratoryDataAccessListener.ConnectEvent {

    private String token;
    private String address;

    public ConnectEventStub(String token, String address) {
        this.token = token;
        this.address = address;
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
    public Map<String, Object> getAdditionalInfo() {
        return null;
    }

    @Override
    public String getTime() {
        return null;
    }
}
