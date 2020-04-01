package com.migratorydata.extensions.authorization;

import java.util.HashMap;
import java.util.Map;

public class Key {

    private Map<String, KeyType> keys = new HashMap<>();

    public void addKey(String key, KeyType type) {
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

    public void removeKey(String key) {
        keys.remove(key);
    }

    public enum KeyType {
        SUBSCRIBE, PUBLISH, PUB_SUB
    }
}
