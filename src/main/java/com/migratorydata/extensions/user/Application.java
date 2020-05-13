package com.migratorydata.extensions.user;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Application {

    private Key key = new Key();
    private Map<String, SubjectType> publicSubjects = new HashMap<>();
    private Map<String, SubjectType> privateSubjects = new HashMap();
    private Map<String, SubjectType> sourceSubjects = new HashMap();
    private Map<String, SubjectType> subscriptionSubjects = new HashMap();

    private User user;

    public Application(User user) {
        this.user = user;
    }

    public void addSubject(String subject, SubjectType subjectType) {
        switch (subjectType) {
            case PUBLIC:
                publicSubjects.put(subject, subjectType);
                break;
            case PRIVATE:
                privateSubjects.put(subject, subjectType);
                break;
            case SOURCE:
                sourceSubjects.put(subject, subjectType);
                break;
            case SUBSCRIPTION:
                subscriptionSubjects.put(subject, subjectType);
                break;
        }
    }

    public void deleteSubject(String subject, SubjectType subjectType) {
        switch (subjectType) {
            case PUBLIC:
                publicSubjects.remove(subject);
                break;
            case PRIVATE:
                privateSubjects.remove(subject);
                break;
            case SOURCE:
                sourceSubjects.remove(subject);
                break;
            case SUBSCRIPTION:
                subscriptionSubjects.remove(subject);
                break;
        }
    }

    public void updateSubject(String oldSubject, SubjectType oldSubjectType, String newSubject, SubjectType newSubjectType) {
        deleteSubject(oldSubject, oldSubjectType);
        addSubject(newSubject, newSubjectType);
    }

    public Key getKey() {
        return key;
    }

    public User getUser() {
        return user;
    }

    public boolean noSubject(String subject) {
        if (publicSubjects.containsKey(subject)) {
            return false;
        }
        if (privateSubjects.containsKey(subject)) {
            return false;
        }
        if (sourceSubjects.containsKey(subject)) {
            return false;
        }
        if (subscriptionSubjects.containsKey(subject)) {
            return false;
        }
        return true;
    }

    public boolean isPrivateSubject(String subject) {
        return privateSubjects.containsKey(subject);
    }

    public boolean isNonPublicSubject(String subject) {
        if (privateSubjects.containsKey(subject)) {
            return true;
        }
        if (sourceSubjects.containsKey(subject)) {
            return true;
        }
        if (subscriptionSubjects.containsKey(subject)) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("\t\t\tPublicSubjects={");
        publicSubjects.forEach((key, value) -> {
            b.append(key).append(",");
        });
        b.append("}\n");

        b.append("\t\t\tPrivateSubjects={");
        privateSubjects.forEach((key, value) -> {
            b.append(key).append(",");
        });
        b.append("}\n");

        b.append("\t\t\tSourceSubjects={");
        sourceSubjects.forEach((key, value) -> {
            b.append(key).append(",");
        });
        b.append("}\n");

        b.append("\t\t\tSubscriptionSubjects={");
        subscriptionSubjects.forEach((key, value) -> {
            b.append(key).append(",");
        });
        b.append("}\n");

        b.append("\t\t\tKeys={");
        b.append(key).append(",");
        b.append("}\n");
        return b.toString();
    }

    public enum SubjectType {
        PUBLIC, PRIVATE, SOURCE, SUBSCRIPTION
    }
}
