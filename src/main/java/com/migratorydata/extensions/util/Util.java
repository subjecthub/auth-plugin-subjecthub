package com.migratorydata.extensions.util;

import com.migratorydata.extensions.user.Application;

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

    public static Application.SubjectType getSubjectTypeByString(String type) {
        switch (type) {
            case "private":
                return Application.SubjectType.PRIVATE;
            case "public":
                return Application.SubjectType.PUBLIC;
            case "source":
                return Application.SubjectType.SOURCE;
            case "subscription":
                return Application.SubjectType.SUBSCRIPTION;
        }
        return Application.SubjectType.PRIVATE;
    }
}
