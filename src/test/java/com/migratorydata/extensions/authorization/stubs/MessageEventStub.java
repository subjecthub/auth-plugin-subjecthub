package com.migratorydata.extensions.authorization.stubs;

import com.migratorydata.extensions.audit.MigratoryDataMessageListener;

import java.util.Map;

public class MessageEventStub implements MigratoryDataMessageListener.MessageEvent {

    private String subject;

    public MessageEventStub(String subject) {
        this.subject = subject;
    }

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public byte[] getContent() {
        return new byte[0];
    }

    @Override
    public int getSeqNo() {
        return 0;
    }

    @Override
    public int getEpochNo() {
        return 0;
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
