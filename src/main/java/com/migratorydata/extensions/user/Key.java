package com.migratorydata.extensions.user;

import com.migratorydata.extensions.util.Util;

import java.util.HashMap;
import java.util.Map;

public class Key {

    private Map<String, KeyType> keys = new HashMap<>();

    public void addKey(String key) {
        KeyType type = Util.getKeyType(key);
        keys.put(key, type);
    }

    public boolean checkSubscribe(String secretKey) {
        KeyType keyType = keys.get(secretKey);
        if (keyType == null) {
            return false;
        }
        if (keyType == KeyType.PUBLISH) {
            return false;
        }
        return true;
    }

    public boolean checkPublish(String secretKey) {
        KeyType keyType = keys.get(secretKey);
        if (keyType == null) {
            return false;
        }
        if (keyType == KeyType.SUBSCRIBE) {
            return false;
        }
        return true;
    }

    public void deleteKey(String key) {
        keys.remove(key);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[");
        keys.forEach((k, v) -> {
            b.append("{").append(k).append("},");
        });
        b.append("]");
        return b.toString();
    }

    public enum KeyType {
        SUBSCRIBE, PUBLISH, PUB_SUB
    }
}
