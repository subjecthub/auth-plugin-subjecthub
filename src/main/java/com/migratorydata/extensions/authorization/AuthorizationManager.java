package com.migratorydata.extensions.authorization;

import com.migratorydata.client.*;
import com.migratorydata.extensions.audit.PublishLimit;
import com.migratorydata.extensions.presence.MigratoryDataPresenceListener;
import com.migratorydata.extensions.user.*;
import com.migratorydata.extensions.util.Util;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import static com.migratorydata.extensions.util.Util.getAppIdAndSecret;
import static com.migratorydata.extensions.util.Util.getSubjecthubId;

public class AuthorizationManager implements MigratoryDataListener, MigratoryDataLogListener, Runnable {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private MigratoryDataClient client = new MigratoryDataClient();
    private Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

    private Map<String, PublishLimit.PublishCount> publishLimit = new HashMap<>();

    private Users users = new Users();

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private String serviceSubject;
    private String serviceToken;
    private String serverName;

    private MySqlAccess mySqlAccess;

    public AuthorizationManager(String cluster, String token, String serviceSubject, String dbConnector, String dbIp,
                                String dbName, String user, String password, String serverName, boolean writeStatsToDB) throws Exception {

        this.serviceSubject = serviceSubject;
        this.serviceToken = token;
        this.serverName = serverName;

        this.mySqlAccess = new MySqlAccess(dbConnector, dbIp, dbName, user, password);

        client.setLogListener(this, MigratoryDataLogLevel.TRACE);
        client.setListener(this);

        client.setEntitlementToken(token);
        client.setServers(new String[]{cluster});
        client.subscribe(Arrays.asList(serviceSubject));
        client.connect();

        executor.scheduleAtFixedRate(() -> {
            queue.offer(() -> {
                try {
                    mySqlAccess.loadUsers(users);
                    String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "DATABASE", "@@@@@@@@       Load from database     @@@@@@@@@"));
                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "DATABASE", users));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }, 0, 360, TimeUnit.SECONDS);

        if (writeStatsToDB) {
            executor.scheduleAtFixedRate(() -> {
                queue.offer(() -> {
                    try {
                        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
                        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "DATABASE", "@@@@@@@@       UPDATE STATISTICS       @@@@@@@@@"));
                        mySqlAccess.updateStats(users.getUsers());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }, 60, 60, TimeUnit.SECONDS);
        }
    }

    public void offer(Runnable r) {
        queue.offer(r);
    }

    @Override
    public void onMessage(MigratoryDataMessage migratoryDataMessage) {
        switch (migratoryDataMessage.getMessageType()) {
            case UPDATE:
            case RECOVERED:
                queue.offer(() -> {
                    JSONObject jsonObject = new JSONObject(new String(migratoryDataMessage.getContent()));

                    String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
                    String logMessage = String.format("[%1$s] [%2$s] %3$s", isoDateTime, "CLIENT_ON_MESSAGE", "Received Json: " + jsonObject.toString());

                    String operation = (String) jsonObject.get("operation");

                    System.out.println(logMessage);

                    // update from Access extension
                    if ("update_connections".equals(operation)) {
                        String server = jsonObject.getString("server");
                        JSONArray counts = jsonObject.getJSONArray("counts");
                        for (int i = 0; i < counts.length(); i++) {
                            String subjecthubId = counts.getJSONObject(i).getString("shid");
                            int count = counts.getJSONObject(i).getInt("count");

                            users.getUser(subjecthubId).countConnections(server, count);
                        }
                    }

                    // update from php PublishJob
                    if ("update_user".equals(operation)) {
                        String subjectHubID = (String) jsonObject.get("subjecthub_id");
                        String opType = (String) jsonObject.get("op_type");
                        if ("add".equals(opType)) {
                            Integer userId = jsonObject.getInt("user_id");
                            Integer connectionsLimit = jsonObject.getInt("connections_limit");
                            Integer publishLimit = jsonObject.getInt("publish_limit");

                            User user = new User(userId, subjectHubID);
                            user.updateLimits(connectionsLimit, publishLimit);
                            users.addUser(subjectHubID, user);
                        } else if ("delete".equals(opType)) {
                            users.deleteUser(subjectHubID);
                        }
                    }

                    if ("update_applications".equals(operation)) {
                        String subjectHubID = (String) jsonObject.get("subjecthub_id");
                        String appId = (String) jsonObject.get("app_id");

                        String opType = (String) jsonObject.get("op_type");
                        if ("add".equals(opType)) {
                            users.addApplication(subjectHubID, appId);
                        } else if ("delete".equals(opType)) {
                            users.deleteApplication(appId);
                        }
                    }

                    if ("update_keys".equals(operation)) {
                        String appId = (String) jsonObject.get("app_id");
                        String publishKey = (String) jsonObject.get("publish_key");
                        String subscribeKey = (String) jsonObject.get("subscribe_key");
                        String pubSubKey = (String) jsonObject.get("pub_sub_key");

                        String type = (String) jsonObject.get("op_type");
                        if ("add".equals(type)) {
                            Key key = users.getKey(appId);
                            if (key != null) {
                                key.addKey(publishKey, Key.KeyType.PUBLISH);
                                key.addKey(subscribeKey, Key.KeyType.SUBSCRIBE);
                                key.addKey(pubSubKey, Key.KeyType.PUB_SUB);
                            }
                        } else if ("delete".equals(type)) {
                            Key key = users.getKey(appId);
                            if (key != null) {
                                key.removeKey(publishKey);
                                key.removeKey(subscribeKey);
                                key.removeKey(pubSubKey);
                            }
                        }
                    }

                    if ("update_subjects".equals(operation)) {
                        String subjectHubID = (String) jsonObject.get("subjecthub_id");
                        String appId = (String) jsonObject.get("app_id");
                        String subject = (String) jsonObject.get("subject");
                        String subjectType = (String) jsonObject.get("subject_type");

                        String type = (String) jsonObject.get("op_type");
                        if ("add".equals(type)) {
                            Application.SubjectType appSubjectType = Util.getSubjectTypeByString(subjectType);
                            if (appSubjectType == Application.SubjectType.PUBLIC) {
                                users.addPublicSubject(subject);
                            }

                            Application application = users.getApplication(appId);
                            application.addSubject(subject, appSubjectType);
                        } else if ("delete".equals(type)) {
                            Application.SubjectType appSubjectType = Util.getSubjectTypeByString(subjectType);

                            users.removePublicSubject(subject);
                            Application application = users.getApplication(appId);
                            if (application != null) {
                                application.deleteSubject(subject, appSubjectType);
                            }
                        }
                    }

                    if ("update_sources".equals(operation)) {
                        Integer sourceId = (Integer) jsonObject.get("source_id");

                        String type = (String) jsonObject.get("op_type");
                        if ("add".equals(type)) {
                            String subjectHubID = (String) jsonObject.get("subjecthub_id");
                            String configuration = (String) jsonObject.get("configuration");
                            String endpoint = (String) jsonObject.get("endpoint");
                            String mdSubject = (String) jsonObject.get("md_subject");
                            String status = (String) jsonObject.get("status");
                            KafkaConnector source = new KafkaConnector(subjectHubID, configuration, endpoint, mdSubject, status);
                            users.addSource(sourceId, source);
                            JSONObject linkObject = new JSONObject();
                            linkObject.put("operation", "link-kafka-to-subjecthub");
                            linkObject.put("subjecthubTopic", source.getMigratoryDataSubject());
                            linkObject.put("kafkaTopic", source.getEndpoint());
                            client.publish(new MigratoryDataMessage(source.getConfigurationSubject(), linkObject.toString().getBytes()));
                        } else if ("delete".equals(type)) {
                            KafkaConnector sourceToRemove = users.getSourceById(sourceId);
                            if (users.getSourcesConfigurationSubjectCount(sourceToRemove.getConfigurationSubject()) == 1) {
                                client.unsubscribe(Collections.singletonList(sourceToRemove.getConfigurationSubject()));
                            }
                            users.removeSource(sourceId);
                            JSONObject unlinkObject = new JSONObject();
                            unlinkObject.put("operation", "unlink-kafka-from-subjecthub");
                            unlinkObject.put("subjecthubTopic", sourceToRemove.getMigratoryDataSubject());
                            unlinkObject.put("kafkaTopic", sourceToRemove.getEndpoint());
                            client.publish(new MigratoryDataMessage(sourceToRemove.getConfigurationSubject(), unlinkObject.toString().getBytes()));
                        }
                    }

                    if ("update_subscriptions".equals(operation)) {
                        Integer subscriptionId = (Integer) jsonObject.get("subscription_id");

                        String type = (String) jsonObject.get("op_type");
                        if ("add".equals(type)) {
                            String subjectHubID = (String) jsonObject.get("subjecthub_id");
                            String configuration = (String) jsonObject.get("configuration");
                            String endpoint = (String) jsonObject.get("endpoint");
                            String mdSubject = (String) jsonObject.get("md_subject");
                            String status = (String) jsonObject.get("status");
                            KafkaConnector subscription = new KafkaConnector(subjectHubID, configuration, endpoint, mdSubject, status);
                            users.addSubscription(subscriptionId, subscription);
                            JSONObject linkObject = new JSONObject();
                            linkObject.put("operation", "link-subjecthub-to-kafka");
                            linkObject.put("subjecthubTopic", subscription.getMigratoryDataSubject());
                            linkObject.put("kafkaTopic", subscription.getEndpoint());
                            client.publish(new MigratoryDataMessage(subscription.getConfigurationSubject(), linkObject.toString().getBytes()));
                        } else if ("delete".equals(type)) {
                            KafkaConnector subscriptionToRemove = users.getSubscriptionById(subscriptionId);
                            if (users.getSubscriptionsConfigurationSubjectCount(subscriptionToRemove.getConfigurationSubject()) == 1) {
                                client.unsubscribe(Collections.singletonList(subscriptionToRemove.getConfigurationSubject()));
                            }
                            users.removeSubscription(subscriptionId);
                            JSONObject unlinkObject = new JSONObject();
                            unlinkObject.put("operation", "unlink-subjecthub-from-kafka");
                            unlinkObject.put("subjecthubTopic", subscriptionToRemove.getMigratoryDataSubject());
                            unlinkObject.put("kafkaTopic", subscriptionToRemove.getEndpoint());
                            client.publish(new MigratoryDataMessage(subscriptionToRemove.getConfigurationSubject(), unlinkObject.toString().getBytes()));
                        }
                    }

                    if ("update_sources_links".equals(operation)) {
                        String configurationSubject = (String) jsonObject.get("configuration_subject");
                        sendSourcesLinksByConfiguration(configurationSubject, "CREATED");
                    }

                    if ("update_subscriptions_links".equals(operation)) {
                        String configurationSubject = (String) jsonObject.get("configuration_subject");
                        sendSubscriptionsLinksByConfiguration(configurationSubject, "CREATED");
                    }
                });
                break;
        }
    }

    private void sendSourcesLinksByConfiguration(String configurationSubject, String notStatus) {
        JSONObject linkKafkaToMdMultipleRequest = new JSONObject();
        linkKafkaToMdMultipleRequest.put("operation", "link-kafka-to-subjecthub-multiple");
        JSONObject kafkaToMdLinks = new JSONObject();
        for (KafkaConnector source : users.getSources().values()) {
            if (source.getConfigurationSubject().equals(configurationSubject) && !source.getStatus().equals(notStatus)) {
                JSONArray mdTopics;
                if (kafkaToMdLinks.has(source.getEndpoint())) {
                    mdTopics = kafkaToMdLinks.getJSONArray(source.getEndpoint());
                } else {
                    mdTopics = new JSONArray();
                }
                mdTopics.put(source.getMigratoryDataSubject());
                kafkaToMdLinks.put(source.getEndpoint(), mdTopics);
            }
        }
        linkKafkaToMdMultipleRequest.put("kafkaToSubjecthubLinks", kafkaToMdLinks);
        client.publish(new MigratoryDataMessage(configurationSubject, linkKafkaToMdMultipleRequest.toString().getBytes()));
    }

    private void sendSubscriptionsLinksByConfiguration(String configurationSubject, String notStatus) {
        JSONObject linkMdToKafkaMultipleRequest = new JSONObject();
        linkMdToKafkaMultipleRequest.put("operation", "link-subjecthub-to-kafka-multiple");
        JSONObject mdToKafkaLinks = new JSONObject();
        for (KafkaConnector subscription : users.getSubscriptions().values()) {
            if (subscription.getConfigurationSubject().equals(configurationSubject) && !subscription.getStatus().equals(notStatus)) {
                JSONArray kafkaTopics;
                if (mdToKafkaLinks.has(subscription.getMigratoryDataSubject())) {
                    kafkaTopics = mdToKafkaLinks.getJSONArray(subscription.getMigratoryDataSubject());
                } else {
                    kafkaTopics = new JSONArray();
                }
                kafkaTopics.put(subscription.getEndpoint());
                mdToKafkaLinks.put(subscription.getMigratoryDataSubject(), kafkaTopics);
            }
        }
        linkMdToKafkaMultipleRequest.put("subjecthubToKafkaLinks", mdToKafkaLinks);
        client.publish(new MigratoryDataMessage(configurationSubject, linkMdToKafkaMultipleRequest.toString().getBytes()));
    }

    public void onConnectorMessage(MigratoryDataPresenceListener.Message message) {
        JSONObject jsonObject = new JSONObject(new String(message.getContent()));

        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        String logMessage = String.format("[%1$s] [%2$s] %3$s", isoDateTime, "CONNECTOR_ON_MESSAGE", "Received Json: " + jsonObject.toString());

        System.out.println(logMessage);

        String operation = (String) jsonObject.get("operation");

        if ("sink-connector-up".equals(operation)) {
            sendSourcesLinksByConfiguration(message.getSubject(), "");
        }

        if ("source-connector-up".equals(operation)) {
            sendSubscriptionsLinksByConfiguration(message.getSubject(), "");
        }

        if ("link-kafka-to-subjecthub-response".equals(operation)) {
            String mdTopic = (String) jsonObject.get("subjecthubTopic");
            String kafkaTopic = (String) jsonObject.get("kafkaTopic");
            String status = (String) jsonObject.get("status");
            if (status.equals("ok") || status.equals("link-already-exists")) {
                Map<Integer, KafkaConnector> sources = users.getSources();
                for (Integer key : sources.keySet()) {
                    KafkaConnector source = sources.get(key);
                    if (source.getStatus().equals("PENDING") && source.getMigratoryDataSubject().equals(mdTopic) &&
                            source.getEndpoint().equals(kafkaTopic) && source.getConfigurationSubject().equals(message.getSubject())) {
                        mySqlAccess.updateSourceStatusById(key, "CREATED");
                        source.setStatus("CREATED");
                        break;
                    }
                }
            }
        }

        if ("link-subjecthub-to-kafka-response".equals(operation)) {
            String mdTopic = (String) jsonObject.get("subjecthubTopic");
            String kafkaTopic = (String) jsonObject.get("kafkaTopic");
            String status = (String) jsonObject.get("status");
            if (status.equals("ok") || status.equals("link-already-exists")) {
                Map<Integer, KafkaConnector> subscriptions = users.getSubscriptions();
                for (Integer key : subscriptions.keySet()) {
                    KafkaConnector subscription = subscriptions.get(key);
                    if (subscription.getStatus().equals("PENDING") && subscription.getMigratoryDataSubject().equals(mdTopic) &&
                            subscription.getEndpoint().equals(kafkaTopic) && subscription.getConfigurationSubject().equals(message.getSubject())) {
                        mySqlAccess.updateSubscriptionsStatusById(key, "CREATED");
                        subscription.setStatus("CREATED");
                        break;
                    }
                }
            }
        }

        if ("link-kafka-to-subjecthub-multiple-response".equals(operation)) {
            JSONObject createdLinks = jsonObject.getJSONObject("createdLinks");
            for (String kafkaTopic : createdLinks.keySet()) {
                JSONArray mdTopics = createdLinks.getJSONArray(kafkaTopic);
                for (Object mdTopic : mdTopics) {
                    Map<Integer, KafkaConnector> sources = users.getSources();
                    for (Integer id : sources.keySet()) {
                        KafkaConnector source = sources.get(id);
                        if (source.getStatus().equals("PENDING") && source.getMigratoryDataSubject().equals(mdTopic) &&
                                source.getEndpoint().equals(kafkaTopic) && source.getConfigurationSubject().equals(message.getSubject())) {
                            mySqlAccess.updateSourceStatusById(id, "CREATED");
                            source.setStatus("CREATED");
                            break;
                        }
                    }
                }
            }
        }

        if ("link-subjecthub-to-kafka-multiple-response".equals(operation)) {
            JSONObject createdLinks = jsonObject.getJSONObject("createdLinks");
            for (String mdTopic : createdLinks.keySet()) {
                JSONArray kafkaTopics = createdLinks.getJSONArray(mdTopic);
                for (Object kafkaTopic : kafkaTopics) {
                    Map<Integer, KafkaConnector> subscriptions = users.getSubscriptions();
                    for (Integer id : subscriptions.keySet()) {
                        KafkaConnector subscription = subscriptions.get(id);
                        if (subscription.getStatus().equals("PENDING") && subscription.getMigratoryDataSubject().equals(mdTopic) &&
                                subscription.getEndpoint().equals(kafkaTopic) && subscription.getConfigurationSubject().equals(message.getSubject())) {
                            mySqlAccess.updateSubscriptionsStatusById(id, "CREATED");
                            subscription.setStatus("CREATED");
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    public void onStatus(String status, String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "CLIENT", "Client status=" + status + ", info=" + info));
    }

    @Override
    public void onLog(String log, MigratoryDataLogLevel level) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, level, log));
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

    public void updateAccessLimit(Map<String, Integer> copyAppCountClients) {
        offer(() -> {
            // broadcast information
            Map<String, Integer> countConnections = new HashMap<>();
            for (Map.Entry<String, Integer> entry : copyAppCountClients.entrySet()) {
                String appId = entry.getKey();
                Integer count = entry.getValue();

                Application application = users.getApplication(appId);
                if (application == null) {
                    continue;
                }
                User user = application.getUser();
                Integer connections = countConnections.get(user.getSubjecthubId());
                if (connections == null) {
                    countConnections.put(user.getSubjecthubId(), count);
                } else {
                    countConnections.put(user.getSubjecthubId(), count + connections);
                }
            }

            JSONArray jsonArray = new JSONArray();
            for (Map.Entry<String, Integer> entry : countConnections.entrySet()) {
                String subjecthubId = entry.getKey();
                Integer count = entry.getValue();
                JSONObject obj = new JSONObject();
                obj.put("shid", subjecthubId);
                obj.put("count", count.intValue());

                jsonArray.put(obj);
            }

            JSONObject updateConnections = new JSONObject();
            updateConnections.put("operation", "update_connections");
            updateConnections.put("counts", jsonArray);
            updateConnections.put("server", serverName);

            client.publish(new MigratoryDataMessage(serviceSubject, updateConnections.toString().getBytes()));
        });
    }

    public void updatePublishLimit(Map<String, PublishLimit.PublishCount> copyPublishLimit) {
        offer(() -> {
            publishLimit = copyPublishLimit;

            // subjecthub_id -> numberOfMessages
            for (Map.Entry<String, PublishLimit.PublishCount> entry : copyPublishLimit.entrySet()) {
                String subjecthubId = entry.getKey();
                PublishLimit.PublishCount publishCount = entry.getValue();
                int newMessages = publishCount.current - publishCount.previous;
                User user = users.getUser(subjecthubId);
                if (user != null) {
                    user.addNewReceivedMessages(newMessages);
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

                if (serviceToken.equals(token)) {
                    migratoryDataSubscribeRequest.setAllowed(subject, true);
                    continue;
                }

                if (subject.equals("/__migratorydata__/presence")) {
                    migratoryDataSubscribeRequest.setAllowed(subject, true);
                    continue;
                }

                // check invalid token
                String[] appIdAndSecret = getAppIdAndSecret(token);
                if (appIdAndSecret == null) {
                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

                // temporary token for website live view, allow subscribe
                if ("app_id_live".equals(appIdAndSecret[0])) {
                    migratoryDataSubscribeRequest.setAllowed(subject, true);
                    continue;
                }

                // check if application is created
                Application application = users.getApplication(appIdAndSecret[0]);
                if (application == null) {
                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    continue;
                }

                // check if connections limit exceeded
                if (application.getUser().isConnectionsLimitExceeded()) {
                    migratoryDataSubscribeRequest.setAllowed(subject, false);
                    String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MANAGER_THREAD", "Connections limit reached for subjecthubId=" + application.getUser().getSubjecthubId()));
                    continue;
                }

                // check key
                boolean allowSubscribe = false;
                if (users.isPublicSubject(subject)) {
                    allowSubscribe = true;
                } else {
                    // check if non public subject is created
                    if (application.isNonPublicSubject(subject)) {
                        Key key = users.getKey(appIdAndSecret[0]);
                        if (key != null && key.checkSubscribe(appIdAndSecret[1])) {
                            allowSubscribe = true;
                        }
                    }
                }

                migratoryDataSubscribeRequest.setAllowed(subject, allowSubscribe);
            }

            migratoryDataSubscribeRequest.sendResponse();
        });
    }

    public void handlePublishCheck(MigratoryDataPublishRequest migratoryDataPublishRequest) {

        if (serviceToken.equals(migratoryDataPublishRequest.getClientCredentials().getToken())) {
            migratoryDataPublishRequest.setAllowed(true);
            migratoryDataPublishRequest.sendResponse();
            return;
        }

        offer(() -> {

            String subject = migratoryDataPublishRequest.getSubject();
            String token = migratoryDataPublishRequest.getClientCredentials().getToken();

            // invalid token format
            String[] appIdAndSecret = getAppIdAndSecret(token);
            if (appIdAndSecret == null) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            // check subject is created
            Application application = users.getApplication(appIdAndSecret[0]);
            if (application == null || application.noSubject(subject)) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            // check key
            Key key = users.getKey(appIdAndSecret[0]);
            if (key == null || key.checkPublish(appIdAndSecret[1]) == false) {
                migratoryDataPublishRequest.setAllowed(false);
                migratoryDataPublishRequest.sendResponse();
                return;
            }

            // check if publish limit exceeded
            boolean allowToPublish = false;
            String subjecthubId = getSubjecthubId(subject);
            if (subjecthubId != null) {
                PublishLimit.PublishCount limit = publishLimit.get(subjecthubId);
                if (limit == null || !users.getUser(subjecthubId).isPublishLimitExceeded(limit.current)) {
                    allowToPublish = true;
                } else {
                    String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MANAGER_THREAD", "Publish Limit reached for subjecthubId=" + subjecthubId));
                }
            }

            migratoryDataPublishRequest.setAllowed(allowToPublish);
            migratoryDataPublishRequest.sendResponse();
        });
    }
}
