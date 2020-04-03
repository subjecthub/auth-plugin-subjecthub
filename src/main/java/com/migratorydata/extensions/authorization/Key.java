package com.migratorydata.extensions.authorization;

import java.util.HashMap;
import java.util.Map;

public class Key {

    private Map<String, Map<String, KeyType>> groupTokeys = new HashMap<>();

    public void addKey(String group, String key, KeyType type) {
        Map<String, KeyType> keys = groupTokeys.get(group);
        if (keys == null) {
            keys = new HashMap<>();
            groupTokeys.put(group, keys);
        }
        keys.put(key, type);
    }

    public boolean checkSubscribe(String group, String secretKey) {
        Map<String, KeyType> keys = groupTokeys.get(group);
        if (keys == null) {
            return false;
        }

        KeyType keyType = keys.get(secretKey);
        if (keyType == null) {
            return false;
        }
        if (keyType == KeyType.PUBLISH) {
            return false;
        }
        return true;
    }

    public boolean checkPublish(String group, String secretKey) {
        Map<String, KeyType> keys = groupTokeys.get(group);
        if (keys == null) {
            return false;
        }

        KeyType keyType = keys.get(secretKey);
        if (keyType == null) {
            return false;
        }
        if (keyType == KeyType.SUBSCRIBE) {
            return false;
        }
        return true;
    }

    public void removeKey(String group, String key) {
        Map<String, KeyType> keys = groupTokeys.get(group);
        if (keys != null) {
            keys.remove(key);
        }
    }

    public void renameGroup(String oldGroup, String newGroup) {
        Map<String, KeyType> keys = groupTokeys.remove(oldGroup);
        if (keys != null) {
            groupTokeys.put(newGroup, keys);
        }
    }

    public void deleteGroup(String group) {
        groupTokeys.remove(group);
    }

    public enum KeyType {
        SUBSCRIBE, PUBLISH, PUB_SUB
    }
}
