package com.migratorydata.extensions.util;

public class Util {

    public static String getSubjecthubId(String subject) {
        int index = subject.indexOf("/", 1);
        if (index == -1) {
            return null;
        }
        return subject.substring(1, index);
    }

    public static String[] getAppIdAndSecret(String token) {
        String[] elements = token.split(":");

        if (elements.length == 2) {
            return elements;
        }

        return null;
    }
}
