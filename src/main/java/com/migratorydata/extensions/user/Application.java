package com.migratorydata.extensions.user;

import java.util.HashMap;
import java.util.Map;

public class Application {

    private Key key = new Key();
    private Map<String, SubjectType> publicSubjects = new HashMap<>();
    private Map<String, SubjectType> privateSubjects = new HashMap();

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
        }
    }

    public void updateSubject(String oldSubject, SubjectType oldSubjectType, String newSubject, SubjectType subjectType) {
        deleteSubject(oldSubject, oldSubjectType);
        addSubject(newSubject, subjectType);
    }

    public boolean containsPrivateSubject(String subject) {
        return privateSubjects.containsKey(subject);
    }

    public boolean containsPublicSubject(String subject) {
        return publicSubjects.containsKey(subject);
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

        b.append("\t\t\tKeys={");
        b.append(key).append(",");
        b.append("}\n");
        return b.toString();
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
        return true;
    }

    public boolean isPrivateSubject(String subject) {
        return privateSubjects.containsKey(subject);
    }

    public enum SubjectType {
        PUBLIC, PRIVATE
    }
}
