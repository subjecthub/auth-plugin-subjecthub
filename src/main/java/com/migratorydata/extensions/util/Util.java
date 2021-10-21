package com.migratorydata.extensions.util;

import com.migratorydata.extensions.user.Key;

public class Util {

    public static String getTopicFromSubject(String subject) {
        int index = subject.indexOf("/", 1);
        if (index == -1) {
            return subject.substring(1);
        }
        return subject.substring(1, index);
    }

    // topic:randomId:access
    public static String[] getKeyElements(String token) {
        String[] elements = token.split(":");

        if (elements.length == 3) {
            return elements;
        }

        return null;
    }

    public static Key.KeyType getKeyType(String key) {
        String[] keyElements = getKeyElements(key);
        if (keyElements.length < 3) {
            throw new RuntimeException("invalid key");
        }
        if (keyElements[2].equals("s.p")) {
            return Key.KeyType.PUB_SUB;
        } else if (keyElements[2].equals("s")) {
            return Key.KeyType.SUBSCRIBE;
        } else if (keyElements[2].equals("p")) {
            return Key.KeyType.PUBLISH;
        }
        throw new RuntimeException("invalid key");
    }
}
