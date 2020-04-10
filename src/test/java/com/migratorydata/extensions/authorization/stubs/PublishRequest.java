package com.migratorydata.extensions.authorization.stubs;

import com.migratorydata.extensions.authorization.MigratoryDataEntitlementListener;
import com.migratorydata.extensions.authorization.MigratoryDataPublishRequest;

public class PublishRequest implements MigratoryDataPublishRequest{

    public boolean allowed = false;
    public boolean sendResponse = false;

    private ClientCredentials clientCredentials;
    private String subject;

    public PublishRequest(ClientCredentials clientCredentials, String subject) {
        this.clientCredentials = clientCredentials;
        this.subject = subject;
    }

    @Override
    public MigratoryDataEntitlementListener.ClientCredentials getClientCredentials() {
        return clientCredentials;
    }

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public void setAllowed(boolean b) {
        this.allowed = b;
    }

    @Override
    public void sendResponse() {
        sendResponse = true;
    }
}
