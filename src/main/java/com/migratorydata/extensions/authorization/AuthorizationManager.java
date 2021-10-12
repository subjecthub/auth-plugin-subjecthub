package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.user.*;
import com.migratorydata.extensions.util.Util;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import static com.migratorydata.extensions.util.Util.getKeyElements;

public class AuthorizationManager implements Runnable {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

//    private Map<String, PublishLimit.PublishCount> publishLimit = new HashMap<>();

    private Map<String, Topic> topics = new HashMap<>();

    public void offer(Runnable r) {
        queue.offer(r);
    }

    public void onMessage(String message) {
        queue.offer(() -> {
            JSONObject jsonMessage = new JSONObject(message);

            String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
            String logMessage = String.format("[%1$s] [%2$s] %3$s", isoDateTime, "CONSUMER_ON_MESSAGE", "Received Json: " + jsonMessage.toString());
            System.out.println(logMessage);

            String operation = jsonMessage.getString("op");
            String topic = jsonMessage.getString("topic");

            if (topics.containsKey(topic) == false) {
                topics.put(topic, new Topic());
            }

            if ("add_key".equals(operation)) {
                topics.get(topic).addKey(jsonMessage.getString("key"));
            }
            if ("delete_key".equals(operation)) {
                topics.get(topic).deleteKey(jsonMessage.getString("key"));
            }
            if ("add_limit".equals(operation)) {
                topics.get(topic).addLimit(new Limit(jsonMessage.getInt("connections"), jsonMessage.getInt("messages")));
            }
            if ("delete_topic".equals(operation)) {
                topics.remove(topic);
            }
        });
    }

    @Override
    public void run() {
        while (true) {
            Runnable r = queue.poll();
            if (r != null) {
                try {
                    r.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

//    public void updateAccessLimit(Map<String, Integer> copyAppCountClients) {
//        offer(() -> {
//            // broadcast information
//            Map<String, Integer> countConnections = new HashMap<>();
//            for (Map.Entry<String, Integer> entry : copyAppCountClients.entrySet()) {
//                String appId = entry.getKey();
//                Integer count = entry.getValue();
//
//                Topic topic = users.getApplication(appId);
//                if (topic == null) {
//                    continue;
//                }
//                User user = topic.getUser();
//                Integer connections = countConnections.get(user.getSubjecthubId());
//                if (connections == null) {
//                    countConnections.put(user.getSubjecthubId(), count);
//                } else {
//                    countConnections.put(user.getSubjecthubId(), count + connections);
//                }
//            }
//
//            JSONArray jsonArray = new JSONArray();
//            for (Map.Entry<String, Integer> entry : countConnections.entrySet()) {
//                String subjecthubId = entry.getKey();
//                Integer count = entry.getValue();
//                JSONObject obj = new JSONObject();
//                obj.put("shid", subjecthubId);
//                obj.put("count", count.intValue());
//
//                jsonArray.put(obj);
//            }
//
//            JSONObject updateConnections = new JSONObject();
//            updateConnections.put("operation", "update_connections");
//            updateConnections.put("counts", jsonArray);
//            updateConnections.put("server", serverName);
//
//            client.publish(new MigratoryDataMessage(serviceSubject, updateConnections.toString().getBytes()));
//        });
//    }
//
//    public void updatePublishLimit(Map<String, PublishLimit.PublishCount> copyPublishLimit) {
//        offer(() -> {
//            publishLimit = copyPublishLimit;
//
//            // subjecthub_id -> numberOfMessages
//            for (Map.Entry<String, PublishLimit.PublishCount> entry : copyPublishLimit.entrySet()) {
//                String subjecthubId = entry.getKey();
//                PublishLimit.PublishCount publishCount = entry.getValue();
//                int newMessages = publishCount.current - publishCount.previous;
//                User user = users.getUser(subjecthubId);
//                if (user != null) {
//                    user.addNewReceivedMessages(newMessages);
//                }
//            }
//        });
//    }

    public void handleSubscribeCheck(MigratoryDataSubscribeRequest migratoryDataSubscribeRequest) {
        offer(() -> {
            String token = migratoryDataSubscribeRequest.getClientCredentials().getToken();
            List<String> subjects = migratoryDataSubscribeRequest.getSubjects();
            for (String subject : subjects) {
                // auth service client

                // check invalid token
                String[] key = getKeyElements(token);
                if (key == null) {
                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

                // check if topic is created
                Topic topic = topics.get(key[0]);
                if (topic == null) {
                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

                // check if connections limit exceeded
//                if (topic.getUser().isConnectionsLimitExceeded()) {
//                    migratoryDataSubscribeRequest.setAllowed(subject, false);
//                    String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
//                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MANAGER_THREAD", "Connections limit reached for subjecthubId=" + topic.getUser().getSubjecthubId()));
//                    continue;
//                }

                // check the key
                if (topic.getKey().checkSubscribe(token)) {
                    migratoryDataSubscribeRequest.setAllowed(subject, true);
                }
            }

            migratoryDataSubscribeRequest.sendResponse();
        });
    }

    public void handlePublishCheck(MigratoryDataPublishRequest migratoryDataPublishRequest) {

        offer(() -> {

            String subject = migratoryDataPublishRequest.getSubject();
            String token = migratoryDataPublishRequest.getClientCredentials().getToken();

            // invalid token format
            String[] key = getKeyElements(token);
            if (key == null) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            // check subject is created
            Topic topic = topics.get(key[0]);
            if (topic == null) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            if (!topic.equals(Util.getTopicFromSubject(subject))) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

//            Key key = users.getKey(key[0]);
//            if (key == null || key.checkPublish(key[1]) == false) {
//                migratoryDataPublishRequest.setAllowed(false);
//                migratoryDataPublishRequest.sendResponse();
//                return;
//            }

            // check if publish limit exceeded
            boolean allowToPublish = false;
//            String subjecthubId = getTopicFromSubject(subject);
//            if (subjecthubId != null) {
//                PublishLimit.PublishCount limit = publishLimit.get(subjecthubId);
//                if (limit == null || !users.getUser(subjecthubId).isPublishLimitExceeded(limit.current)) {
//                    allowToPublish = true;
//                } else {
//                    String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
//                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MANAGER_THREAD", "Publish Limit reached for subjecthubId=" + subjecthubId));
//                }
//            }

            // check key
            if (topic.getKey().checkPublish(token)) {
                allowToPublish = true;
            }

            migratoryDataPublishRequest.setAllowed(allowToPublish);
            migratoryDataPublishRequest.sendResponse();
        });
    }
}
