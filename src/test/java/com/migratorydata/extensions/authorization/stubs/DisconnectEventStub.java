package com.migratorydata.extensions.authorization.stubs;

import com.migratorydata.extensions.audit.MigratoryDataAccessListener;

import java.util.Map;

public class DisconnectEventStub implements MigratoryDataAccessListener.DisconnectEvent {

    private String address;

    public DisconnectEventStub(String address) {
        this.address = address;
    }

    @Override
    public String getClientAddress() {
        return address;
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
