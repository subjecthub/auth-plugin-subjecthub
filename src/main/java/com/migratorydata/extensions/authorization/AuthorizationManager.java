package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.user.*;
import com.migratorydata.extensions.util.Util;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

import static com.migratorydata.extensions.util.Util.getKeyElements;

public class AuthorizationManager implements Runnable {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

    private Map<String, Topic> topics = new HashMap<>();

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public AuthorizationManager() {
        LocalDateTime start = LocalDateTime.now();
        LocalDateTime end = start.plusHours(1).truncatedTo(ChronoUnit.HOURS);

        Duration duration = Duration.between(start, end);
        long nextHour = duration.getSeconds();

        executor.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, Topic> entry : topics.entrySet()) {
                entry.getValue().resetMessagesNumber();
            }
        }, nextHour, 3600, TimeUnit.SECONDS);
    }

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
                topics.put(topic, new Topic(topic));
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

    public void updateConnections(Map<String, Integer> topicToConnections, String serverName) {
        offer(() -> {
            for (Map.Entry<String, Integer> entry : topicToConnections.entrySet()) {
                Topic topic = topics.get(entry.getKey());
                if (topic != null) {
                    topic.updateConnections(serverName, entry.getValue());
                }
            }
        });
    }

    public void updateMessages(Map<String, Integer> messages, String serverName) {
        offer(() -> {
            for (Map.Entry<String, Integer> entry : messages.entrySet()) {
                Topic topic = topics.get(entry.getKey());
                if (topic != null) {
                    topic.addMessages(entry.getValue());
                }
            }
        });
    }

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

                String topicFromSubject = Util.getTopicFromSubject(subject);
                if (!topicFromSubject.equals(topic.getTopic())) {
                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

                // check if connections limit exceeded
                if (topic.isConnectionLimitExceeded()) {
                    String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MANAGER_THREAD", "Connections Limit reached for topic=" + key[0]));

                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

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

            if (!topic.getTopic().equals(Util.getTopicFromSubject(subject))) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            // check if publish limit exceeded
            if (topic.isMessagesLimitExceeded()) {
                String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
                System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MANAGER_THREAD", "Messages Limit reached for topic=" + key[0]));

                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            // check key
            if (topic.getKey().checkPublish(token)) {
                migratoryDataPublishRequest.setAllowed(true);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            migratoryDataPublishRequest.setAllowed(false);
            migratoryDataPublishRequest.sendResponse();
        });
    }
}
