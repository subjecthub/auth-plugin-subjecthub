package com.migratorydata.extensions.authorization.stubs;

import com.migratorydata.extensions.authorization.MigratoryDataEntitlementListener;
import com.migratorydata.extensions.authorization.MigratoryDataSubscribeRequest;

import java.util.Arrays;
import java.util.List;

public class SubscribeRequest implements MigratoryDataSubscribeRequest {

    public boolean allowed = false;
    public boolean sendResponse = false;

    private ClientCredentials clientCredentials;
    private List<String> subjects;

    public SubscribeRequest(ClientCredentials clientCredentials, String subject) {
        this.clientCredentials = clientCredentials;
        this.subjects = Arrays.asList(subject);
    }

    @Override
    public MigratoryDataEntitlementListener.ClientCredentials getClientCredentials() {
        return clientCredentials;
    }

    @Override
    public List<String> getSubjects() {
        return subjects;
    }

    @Override
    public void setAllowed(String s, boolean b) {
        this.allowed = b;
    }

    @Override
    public void sendResponse() {
        this.sendResponse = true;
    }
}
