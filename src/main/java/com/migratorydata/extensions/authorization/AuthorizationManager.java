package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.user.*;
import com.migratorydata.extensions.util.Metric;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

import static com.migratorydata.extensions.util.Util.getKeyElements;
import static com.migratorydata.extensions.util.Util.getTopicAndApplicationFromSubject;

public class AuthorizationManager implements Runnable {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

    private Map<String, Application> applications = new HashMap<>();

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public AuthorizationManager() {
        LocalDateTime start = LocalDateTime.now();
        LocalDateTime end = start.plusHours(1).truncatedTo(ChronoUnit.HOURS);

        Duration duration = Duration.between(start, end);
        long nextHour = duration.getSeconds();

        executor.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, Application> entry : applications.entrySet()) {
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
            String application = jsonMessage.getString("application");

            if (applications.containsKey(application) == false) {
                applications.put(application, new Application(topic, application));
            }

            if ("add_key".equals(operation)) {
                applications.get(application).addKey(jsonMessage.getString("key"));
            }
            if ("delete_key".equals(operation)) {
                applications.get(application).deleteKey(jsonMessage.getString("key"));
            }
            if ("add_limit".equals(operation)) {
                applications.get(application).addLimit(new Limit(jsonMessage.getInt("connections"), jsonMessage.getInt("messages")));
            }
            if ("delete_topic".equals(operation)) {
                applications.remove(application);
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

    public void updateConnections(Map<String, Metric> topicToConnections, String serverName) {
        offer(() -> {
            for (Map.Entry<String, Metric> entry : topicToConnections.entrySet()) {
                Application application = applications.get(entry.getKey());
                if (application != null) {
                    application.updateConnections(serverName, entry.getValue().value);
                }
            }
        });
    }

    public void updateMessages(Map<String, Metric> messages, String serverName) {
        offer(() -> {
            for (Map.Entry<String, Metric> entry : messages.entrySet()) {
                Application application = applications.get(entry.getKey());
                if (application != null) {
                    application.addMessages(entry.getValue().value);
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
                Application application = applications.get(key[1]);
                if (application == null) {
                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

                String[] topicAndSubject = getTopicAndApplicationFromSubject(subject);
                if (!topicAndSubject[0].equals(application.getTopic())) {
                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

                if (!application.getApplication().equals(topicAndSubject[1])) {
                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

                // check if connections limit exceeded
                if (application.isConnectionLimitExceeded()) {
                    String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MANAGER_THREAD", "Connections Limit reached for user with subject prefix=/" + key[0] + "/" + key[1]));

                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

                // check the key
                if (application.getKey().checkSubscribe(token)) {
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
            Application application = applications.get(key[1]);
            if (application == null) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            String[] topicAndSubject = getTopicAndApplicationFromSubject(subject);
            if (!application.getTopic().equals(topicAndSubject[0])) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            if (!application.getApplication().equals(topicAndSubject[1])) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            // check if publish limit exceeded
            if (application.isMessagesLimitExceeded()) {
                String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
                System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MANAGER_THREAD", "Messages Limit reached for user with subject prefix=/" + key[0] + "/" + key[1]));

                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            // check key
            if (application.getKey().checkPublish(token)) {
                migratoryDataPublishRequest.setAllowed(true);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            migratoryDataPublishRequest.setAllowed(false);
            migratoryDataPublishRequest.sendResponse();
        });
    }
}
