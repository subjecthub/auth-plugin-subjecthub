package com.migratorydata.extensions.authorization;

import java.util.HashMap;
import java.util.Map;

public class Application {

    private Map<String, Boolean> subjects = new HashMap();

    public void addSubject(String subject) {
        subjects.put(subject, Boolean.TRUE);
    }

    public boolean containsSubject(String subject) {
        return subjects.containsKey(subject);
    }

//    public void renameGroup(String subjectHubId, String oldGroup, String newGroup) {
//        Map<String, Boolean> subjects = groupToSubjects.remove(oldGroup);
//        if (subjects != null) {
//            Map<String, Boolean> renameSubjects = new HashMap<>();
//            groupToSubjects.put(newGroup, renameSubjects);
//
//            for (Map.Entry<String, Boolean> subject : subjects.entrySet()) {
//                String subjectSufix = getSubjectSuffix(subject.getKey());
//                renameSubjects.put("/" + subjectHubId + "/" + newGroup + "/" + subjectSufix, Boolean.TRUE);
//            }
//        }
//    }

//    public void deleteApp(String appId) {
//        groupToSubjects.remove(group);
//    }

    public void updateSubject(String oldSubject, String newSubject) {
        subjects.remove(oldSubject);
        subjects.put(newSubject, Boolean.TRUE);
    }

    public void deleteSubject(String subject) {
        subjects.remove(subject);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("{");
        subjects.forEach((key, value) -> {
            b.append("key,");
        });
        b.append("}\n");
        return b.toString();
    }
}
