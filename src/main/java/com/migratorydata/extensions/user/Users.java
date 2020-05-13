package com.migratorydata.extensions.user;

import java.util.HashMap;
import java.util.Map;

public class Users {

    private Map<String, User> users = new HashMap<>(); // subjecthubid -> user
    private Map<String, Application> applications = new HashMap<>(); // app_id -> application

    private Map<String, Application.SubjectType> publicSubjects = new HashMap<>();

    private Map<Integer, KafkaConnector> sources = new HashMap<>();
    private Map<Integer, KafkaConnector> subscriptions = new HashMap<>();

    public Application getApplication(String appId) {
        return applications.get(appId);
    }

    public void addApplication(String subjectHubID, String appId) {
        User user = users.get(subjectHubID);
        applications.put(appId, new Application(user));
    }

    public void deleteApplication(String appId) {
        applications.remove(appId);
    }

    public Key getKey(String appId) {
        Application application = applications.get(appId);
        if (application == null) {
            return null;
        }
        return application.getKey();
    }

    public User getUser(String subjecthubId) {
        return users.get(subjecthubId);
    }

    public void addUser(String subjecthubId, User u) {
        users.put(subjecthubId, u);
    }

    public void deleteUser(String subjecthubId) {
        users.remove(subjecthubId);
    }

    public void addPublicSubject(String completeSubject) {
        publicSubjects.put(completeSubject, Application.SubjectType.PUBLIC);
    }

    public void removePublicSubject(String oldSubject) {
        publicSubjects.remove(oldSubject);
    }

    public boolean isPublicSubject(String subject) {
        return publicSubjects.containsKey(subject);
    }

    public void addSource(Integer id, KafkaConnector source) {
        sources.put(id, source);
    }

    public Map<Integer, KafkaConnector> getSources() {
        return sources;
    }

    public KafkaConnector getSourceById(Integer id) {
        return sources.get(id);
    }

    public int getSourcesConfigurationSubjectCount(String configurationSubject) {
        int count = 0;
        for (KafkaConnector source : sources.values()) {
            if (source.getConfigurationSubject().equals(configurationSubject)) {
                count++;
            }
        }
        return count;
    }

    public void removeSource(Integer id) {
        sources.remove(id);
    }

    public void addSubscription(Integer id, KafkaConnector subscription) {
        subscriptions.put(id, subscription);
    }

    public Map<Integer, KafkaConnector> getSubscriptions() {
        return subscriptions;
    }

    public KafkaConnector getSubscriptionById(Integer id) {
        return subscriptions.get(id);
    }

    public int getSubscriptionsConfigurationSubjectCount(String configurationSubject) {
        int count = 0;
        for (KafkaConnector subscription : subscriptions.values()) {
            if (subscription.getConfigurationSubject().equals(configurationSubject)) {
                count++;
            }
        }
        return count;
    }

    public void removeSubscription(Integer id) {
        subscriptions.remove(id);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("PublicSubjects={");
        publicSubjects.forEach((key, value) -> {
            b.append(key).append(",");
        });
        b.append("}\n");

        b.append("Users={");
        users.forEach((key, value) -> {
            b.append(key).append("(").append(value.getConnectionsCount()).append(")").append(",");
        });
        b.append("}\n");

        b.append("Applications={");
        applications.forEach((key, value) -> {
            b.append("\n\t").append(key).append("\n").append(value);
        });
        b.append("}\n");

        return b.toString();
    }

    public Map<String, User> getUsers() {
        return users;
    }
}
