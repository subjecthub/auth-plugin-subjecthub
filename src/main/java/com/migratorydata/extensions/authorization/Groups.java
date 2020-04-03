package com.migratorydata.extensions.authorization;

import java.util.HashMap;
import java.util.Map;

public class Groups {

    private Map<String, Map<String, Boolean>> groupToSubjects = new HashMap();

    public void addSubject(String group, String subject) {
        Map<String, Boolean> subjects = groupToSubjects.get(group);
        if (subjects == null) {
            subjects = new HashMap<>();
            groupToSubjects.put(group, subjects);
        }
        subjects.put(subject, Boolean.TRUE);
    }

    public boolean containsSubject(String group, String subject) {
        Map<String, Boolean> subjects = groupToSubjects.get(group);
        if (subjects != null) {
            return subjects.containsKey(subject);
        }
        return false;
    }

    public void renameGroup(String subjectHubId, String oldGroup, String newGroup) {
        Map<String, Boolean> subjects = groupToSubjects.remove(oldGroup);
        if (subjects != null) {
            Map<String, Boolean> renameSubjects = new HashMap<>();
            groupToSubjects.put(newGroup, renameSubjects);

            for (Map.Entry<String, Boolean> subject : subjects.entrySet()) {
                String subjectSufix = getSubjectSuffix(subject.getKey());
                renameSubjects.put("/" + subjectHubId + "/" + newGroup + "/" + subjectSufix, Boolean.TRUE);
            }
        }
    }

    public void deleteGroup(String group) {
        groupToSubjects.remove(group);
    }

    public void updateSubject(String group, String oldSubject, String newSubject) {
        Map<String, Boolean> subjects = groupToSubjects.get(group);
        if (subjects == null) {
            subjects = new HashMap<>();
            groupToSubjects.put(group, subjects);
        }
        subjects.remove(oldSubject);
        subjects.put(newSubject, Boolean.TRUE);
    }

    public void deleteSubject(String group, String subject) {
        Map<String, Boolean> subjects = groupToSubjects.get(group);
        if (subjects != null) {
            subjects.remove(subject);
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        groupToSubjects.forEach((key, value) -> {
            b.append(key).append("{");
            value.forEach((k, v) -> b.append(k).append("|||"));
            b.append(key).append("}\n");
        });
        return b.toString();
    }

    public static String getSubjectSuffix(String subject) {
        String secondPart = subject.substring(subject.indexOf("/", 1) + 1);
        return secondPart.substring(secondPart.indexOf("/") + 1);
    }
}
