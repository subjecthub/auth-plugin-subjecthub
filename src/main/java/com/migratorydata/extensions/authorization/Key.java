package com.migratorydata.extensions.authorization;

public class Key {

    private boolean privatePrefix;
    private String publishKey;
    private String subscribeKey;
    private String pubSubKey;

    public Key(boolean privatePrefix, String publishKey, String subscribeKey, String pubSubKey) {
        this.privatePrefix = privatePrefix;
        this.publishKey = publishKey;
        this.subscribeKey = subscribeKey;
        this.pubSubKey = pubSubKey;
    }

    public boolean checkSubscribePrivate(String key) {
        if (key.equals(subscribeKey) || key.equals(pubSubKey)) {
            return true;
        }
        return false;
    }

    public boolean checkPublishPrivate(String key) {
        if (key.equals(publishKey) || key.equals(pubSubKey)) {
            return true;
        }
        return false;
    }

    public boolean checkPublishPublic(String key) {
        return key.equals(publishKey);
    }

    public boolean isPrivatePrefix() {
        return privatePrefix;
    }
}
