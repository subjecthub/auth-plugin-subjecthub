package com.migratorydata.extensions.authorization;

import com.migratorydata.client.*;
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

    private Map<String, Integer> publishLimit = new HashMap<>();

    private Users users = new Users();

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private String serviceSubject;
    private String serviceToken;
    private String serverName;

    private MySqlAccess mySqlAccess;

    public AuthorizationManager(String cluster, String token, String serviceSubject, String dbConnector,
                                String dbIp, String dbName, String user, String password, String serverName) throws Exception {

        this.serviceSubject = serviceSubject;
        this.serviceToken = token;
        this.serverName = serverName;

        this.mySqlAccess = new MySqlAccess(dbConnector, dbIp, dbName, user, password);

        client.setLogListener(this, MigratoryDataLogLevel.DEBUG);
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
                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "DATABASE", "@@@@@@@@Load from database:@@@@@@@@@"));
                    System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "DATABASE", "users"));

                    Set<String> subscribeSubjects = new HashSet<>();
                    for (KafkaConnector source : users.getSources().values()) {
                        subscribeSubjects.add(source.getConfigurationSubject());
                    }
                    for (KafkaConnector subscription : users.getSubscriptions().values()) {
                        subscribeSubjects.add(subscription.getConfigurationSubject());
                    }
                    client.subscribe(new ArrayList<>(subscribeSubjects));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }, 0, 360, TimeUnit.SECONDS);
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

                    if (migratoryDataMessage.getSubject().equals(serviceSubject)) {
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

                            mySqlAccess.saveConnectionsStats(users);
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
                            if ("add".equals(type) || "update".equals(type)) {
                                String subjectHubID = (String) jsonObject.get("subjecthub_id");
                                String configuration = (String) jsonObject.get("configuration");
                                String endpoint = (String) jsonObject.get("endpoint");
                                String mdSubject = (String) jsonObject.get("md_subject");
                                KafkaConnector sourceToAdd = new KafkaConnector(subjectHubID, configuration, endpoint, mdSubject);
                                KafkaConnector previousSource = users.getSourceById(sourceId);
                                if (previousSource != null) {
                                    if (!sourceToAdd.getConfigurationSubject().equals(previousSource.getConfigurationSubject())
                                            && users.getSourcesConfigurationSubjectCount(previousSource.getConfigurationSubject()) == 1) {
                                        client.unsubscribe(Collections.singletonList(previousSource.getConfiguration()));
                                    }
                                }
                                users.addSource(sourceId, sourceToAdd);
                                if (users.getSourcesConfigurationSubjectCount(sourceToAdd.getConfigurationSubject()) == 1) {
                                    client.subscribe(Collections.singletonList(sourceToAdd.getConfigurationSubject()));
                                }
                            } else if ("delete".equals(type)) {
                                KafkaConnector sourceToRemove = users.getSourceById(sourceId);
                                if (users.getSourcesConfigurationSubjectCount(sourceToRemove.getConfigurationSubject()) == 1) {
                                    client.unsubscribe(Collections.singletonList(sourceToRemove.getConfigurationSubject()));
                                }
                                users.removeSource(sourceId);
                            }
                        }

                        if ("update_subscriptions".equals(operation)) {
                            Integer subscriptionId = (Integer) jsonObject.get("subscription_id");

                            String type = (String) jsonObject.get("op_type");
                            if ("add".equals(type) || "update".equals(type)) {
                                String subjectHubID = (String) jsonObject.get("subjecthub_id");
                                String configuration = (String) jsonObject.get("configuration");
                                String endpoint = (String) jsonObject.get("endpoint");
                                String mdSubject = (String) jsonObject.get("md_subject");
                                KafkaConnector subscriptionToAdd = new KafkaConnector(subjectHubID, configuration, endpoint, mdSubject);
                                KafkaConnector previousSubscription = users.getSubscriptionById(subscriptionId);
                                if (previousSubscription != null) {
                                    if (!subscriptionToAdd.getConfigurationSubject().equals(previousSubscription.getConfigurationSubject())
                                            && users.getSubscriptionsConfigurationSubjectCount(previousSubscription.getConfigurationSubject()) == 1) {
                                        client.unsubscribe(Collections.singletonList(previousSubscription.getConfiguration()));
                                    }
                                }
                                users.addSubscription(subscriptionId, subscriptionToAdd);
                                if (users.getSubscriptionsConfigurationSubjectCount(subscriptionToAdd.getConfigurationSubject()) == 1) {
                                    client.subscribe(Collections.singletonList(subscriptionToAdd.getConfigurationSubject()));
                                }
                            } else if ("delete".equals(type)) {
                                KafkaConnector subscriptionToRemove = users.getSubscriptionById(subscriptionId);
                                if (users.getSubscriptionsConfigurationSubjectCount(subscriptionToRemove.getConfigurationSubject()) == 1) {
                                    client.unsubscribe(Collections.singletonList(subscriptionToRemove.getConfigurationSubject()));
                                }
                                users.removeSubscription(subscriptionId);
                            }
                        }
                    } else {

                        if ("sink-connector-up".equals(operation)) {
                            System.out.println(logMessage);
                            JSONObject linkKafkaToMdMultipleRequest = new JSONObject();
                            linkKafkaToMdMultipleRequest.put("operation", "link-kafka-to-migratory-data-multiple");
                            JSONObject kafkaToMdLinks = new JSONObject();
                            for (KafkaConnector source : users.getSources().values()) {
                                if (source.getConfigurationSubject().equals(migratoryDataMessage.getSubject())) {
                                    kafkaToMdLinks.put(source.getEndpoint(), source.getMigratoryDataSubject());
                                }
                            }
                            linkKafkaToMdMultipleRequest.put("kafkaToMdLinks", kafkaToMdLinks);
                            client.publish(new MigratoryDataMessage(migratoryDataMessage.getSubject(), linkKafkaToMdMultipleRequest.toString().getBytes()));
                        }

                        if ("source-connector-up".equals(operation)) {
                            System.out.println(logMessage);
                            JSONObject linkMdToKafkaMultipleRequest = new JSONObject();
                            linkMdToKafkaMultipleRequest.put("operation", "link-migratory-data-to-kafka-multiple");
                            JSONObject mdToKafkaLinks = new JSONObject();
                            for (KafkaConnector subscription : users.getSubscriptions().values()) {
                                if (subscription.getConfigurationSubject().equals(migratoryDataMessage.getSubject())) {
                                    mdToKafkaLinks.put(subscription.getMigratoryDataSubject(), subscription.getEndpoint());
                                }
                            }
                            linkMdToKafkaMultipleRequest.put("mdToKafkaLinks", mdToKafkaLinks);
                            client.publish(new MigratoryDataMessage(migratoryDataMessage.getSubject(), linkMdToKafkaMultipleRequest.toString().getBytes()));
                        }
                    }
                });
                break;
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
            Map<String, User> allUsers = users.getUsers();
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

    public void updatePublishLimit(Map<String, Integer> copyPublishLimit) {
        offer(() -> {
            publishLimit = copyPublishLimit;

            mySqlAccess.saveMessagesStats(users);
        });
    }

    public void handleSubscribeCheck(MigratoryDataSubscribeRequest migratoryDataSubscribeRequest) {
        offer(() -> {
            String token = migratoryDataSubscribeRequest.getClientCredentials().getToken();
            List<String> subjects = migratoryDataSubscribeRequest.getSubjects();
            for (String subject : subjects) {
                // auth service client
                Set<String> connectorsSubjects = new HashSet<>();
                for (KafkaConnector source : users.getSources().values()) {
                    connectorsSubjects.add(source.getConfigurationSubject());
                }
                for (KafkaConnector subscription : users.getSubscriptions().values()) {
                    connectorsSubjects.add(subscription.getConfigurationSubject());
                }
                if (subject.equals(serviceSubject) || (connectorsSubjects.contains(subject) && token.equals(serviceToken))) {
                    if (token.equals(serviceToken)) {
                        migratoryDataSubscribeRequest.setAllowed(subject, true);
                    } else {
                        migratoryDataSubscribeRequest.setAllowed(subject, false);
                    }
                } else {

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
            }

            migratoryDataSubscribeRequest.sendResponse();
        });
    }

    public void handlePublishCheck(MigratoryDataPublishRequest migratoryDataPublishRequest) {

        Set<String> connectorsSubjects = new HashSet<>();
        for (KafkaConnector source : users.getSources().values()) {
            connectorsSubjects.add(source.getConfigurationSubject());
        }
        for (KafkaConnector subscription : users.getSubscriptions().values()) {
            connectorsSubjects.add(subscription.getConfigurationSubject());
        }

        if (migratoryDataPublishRequest.getSubject().equals(serviceSubject) ||
                (connectorsSubjects.contains(migratoryDataPublishRequest.getSubject()) &&
                        migratoryDataPublishRequest.getClientCredentials().getToken().equals(serviceToken))) {
            // allow publish on service subject with service.token
            if (migratoryDataPublishRequest.getClientCredentials().getToken().equals(serviceToken)) {
                migratoryDataPublishRequest.setAllowed(true);
            }

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
                Integer count = publishLimit.get(subjecthubId);
                if (count == null || !users.getUser(subjecthubId).isPublishLimitExceeded(count.intValue())) {
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
