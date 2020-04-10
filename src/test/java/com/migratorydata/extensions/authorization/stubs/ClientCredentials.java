package com.migratorydata.extensions.authorization.stubs;

import com.migratorydata.extensions.authorization.MigratoryDataEntitlementListener;
import com.migratorydata.extensions.authorization.MigratoryDataPublishRequest;

import java.util.Map;

public class ClientCredentials implements MigratoryDataEntitlementListener.ClientCredentials {

    private String token;
    private String address;

    public ClientCredentials(String token, String address) {
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
    public Map<String, Object> getAdditionalInfo() {
        return null;
    }
}
