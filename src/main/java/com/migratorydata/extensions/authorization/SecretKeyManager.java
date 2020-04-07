package com.migratorydata.extensions.authorization;

import com.migratorydata.client.*;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class SecretKeyManager implements MigratoryDataListener, MigratoryDataLogListener, Runnable {

    private static final String authFileName = "authorization.conf";

    private static String token = "some-token";
    private static String serviceSubject = "/migratory/secret";
    private static String cluster = "192.168.1.104:8800";

    private static String dbConnector = "mysql";
    private static String dbIp;
    private static String dbName;
    private static String user;
    private static String password;

    static {
        boolean loadConfig = false;
        Properties prop = null;

        if (loadConfig == false) {
            try (InputStream input = new FileInputStream("/usr/share/migratorydata/extensions/" + authFileName)) {
                System.out.println("load from /usr/share/migratorydata/extensions/" + authFileName);
                prop = new Properties();
                prop.load(input);
                loadConfig = true;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        if (loadConfig == false) {
            try (InputStream input = new FileInputStream("./extensions/" + authFileName)) {
                System.out.println("load from ./extensions/" + authFileName);
                prop = new Properties();
                prop.load(input);
                loadConfig = true;
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        }

        if (loadConfig == false) {
            try (InputStream input = new FileInputStream("./" + authFileName)) {
                System.out.println("load from ./" + authFileName);
                prop = new Properties();
                prop.load(input);
                loadConfig = true;
            } catch (IOException exx) {
                exx.printStackTrace();
            }
        }

        if (loadConfig == false) {
            try (InputStream input = SecretKeyManager.class.getClassLoader().getResourceAsStream(authFileName)) {
                System.out.println("load from resources = " + authFileName);
                prop = new Properties();
                prop.load(input);
                loadConfig = true;
            } catch (IOException exxx) {
                exxx.printStackTrace();
            }
        }

        if (loadConfig) {
            token = prop.getProperty("service.token");
            serviceSubject = prop.getProperty("service.subject");
            cluster = prop.getProperty("service.cluster");

            dbConnector = prop.getProperty("db.connector");
            dbIp = prop.getProperty("db.ip");
            dbName = prop.getProperty("db.name");
            user = prop.getProperty("db.user");
            password = prop.getProperty("db.password");
        }
    }

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private MigratoryDataClient client = new MigratoryDataClient();
    private Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

    private Map<String, Key> keys = new HashMap<>(); // subject -> secret_key
    private Map<String, Groups> publicSubjects = new HashMap<>();

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public SecretKeyManager() {
        client.setLogListener(this, MigratoryDataLogLevel.DEBUG);
        client.setListener(this);

        client.setEntitlementToken(token);
        client.setServers(new String[] { cluster });
        client.subscribe(Arrays.asList(serviceSubject));
        client.connect();

        executor.scheduleAtFixedRate(() -> {
            queue.offer(() -> {
               MySqlAccess mySqlAccess = new MySqlAccess();
                try {
                    Map<String, Key> subjectToKey = mySqlAccess.readKeysFromDataBase(dbConnector, dbIp, dbName, user, password);
                    if (subjectToKey.size() > 0) {
                        keys.putAll(subjectToKey);
                    }

                    publicSubjects.putAll(mySqlAccess.readPublicSubjectsFromDataBase(dbConnector, dbIp, dbName, user, password));
                    //publicSubjects.forEach((key, value) -> System.out.println(key + ":" + value));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }, 0, 360, TimeUnit.SECONDS);
    }

    public void offer(Runnable r) {
        queue.offer(r);
    }

    public String getServiceToken() {
        return token;
    }

    public String getServiceSubject() {
        return serviceSubject;
    }

    public Key getGroupKeys(String group) {
        return keys.get(group);
    }

    public boolean isPublicSubject(String subjectHubId, String group, String subject) {
        Groups groups = publicSubjects.get(subjectHubId);
        if (groups != null) {
            return groups.containsSubject(group, subject);
        }
        return false;
    }

    @Override
    public void onMessage(MigratoryDataMessage migratoryDataMessage) {
        switch (migratoryDataMessage.getMessageType()) {
            case UPDATE:
            case RECOVERED:
                queue.offer(() -> {
                    JSONObject jsonObject = new JSONObject(new String(migratoryDataMessage.getContent()));

                    System.out.println("Received Json: " + jsonObject.toString());

                    String operation = (String) jsonObject.get("operation");

                    if ("update_groups".equals(operation)) {
                        String subjectHubID = (String) jsonObject.get("subjecthub_id");
                        String group = (String) jsonObject.get("group");

                        String opType = (String) jsonObject.get("op_type");
                        if ("update".equals(opType)) {
                            String oldGroup = (String) jsonObject.get("old_group");
                            Key key = keys.get(subjectHubID);
                            if (key != null) {
                                key.renameGroup(oldGroup, group);
                            }

                            Groups groups = publicSubjects.get(subjectHubID);
                            if (groups != null) {
                                groups.renameGroup(subjectHubID, oldGroup, group);
                            }
                        } else if ("delete".equals(opType)) {
                            Key key = keys.get(subjectHubID);
                            if (key != null) {
                                key.deleteGroup(group);
                            }

                            Groups groups = publicSubjects.get(subjectHubID);
                            if (groups != null) {
                                groups.deleteGroup(group);
                            }
                        }
                    }

                    if ("update_keys".equals(operation)) {
                        String subjectHubID = (String) jsonObject.get("subjecthub_id");
                        String group = (String) jsonObject.get("group");
                        String publishKey = (String) jsonObject.get("publish_key");
                        String subscribeKey = (String) jsonObject.get("subscribe_key");
                        String pubSubKey = (String) jsonObject.get("pub_sub_key");

                        String type = (String) jsonObject.get("op_type");
                        if ("add".equals(type)) {
                            Key key = keys.get(subjectHubID);
                            if (key == null) {
                                key = new Key();
                                keys.put(subjectHubID, key);
                            }
                            key.addKey(group, publishKey, Key.KeyType.PUBLISH);
                            key.addKey(group, subscribeKey, Key.KeyType.SUBSCRIBE);
                            key.addKey(group, pubSubKey, Key.KeyType.PUB_SUB);
                        } else if ("delete".equals(type)) {
                            Key key = keys.get(subjectHubID);
                            if (key != null) {
                                key.removeKey(group, publishKey);
                                key.removeKey(group, subscribeKey);
                                key.removeKey(group, pubSubKey);
                            }
                        }
                    }

                    if ("update_public_subjects".equals(operation)) {
                        String subjectHubID = (String) jsonObject.get("subjecthub_id");
                        String group = (String) jsonObject.get("group");
                        String subject = (String) jsonObject.get("subject");

                        String type = (String) jsonObject.get("op_type");
                        if ("add".equals(type)) {
                            Groups groups = publicSubjects.get(subjectHubID);
                            if (groups == null) {
                                groups = new Groups();
                                publicSubjects.put(subjectHubID, groups);
                            }
                            groups.addSubject(group, subject);
                        } else if ("update".equals(type)) {
                            String oldSubject = (String) jsonObject.get("old_subject");
                            Groups groups = publicSubjects.get(subjectHubID);
                            if (groups != null) {
                                groups.updateSubject(group, oldSubject, subject);
                            }
                        } else if ("delete".equals(type)) {
                            Groups groups = publicSubjects.get(subjectHubID);
                            if (groups != null) {
                                groups.deleteSubject(group, subject);
                            }
                        }
                    }
                });
                break;
        }
    }

    @Override
    public void onStatus(String status, String info) {
        System.out.println("Client status=" + status + ", info=" + info);
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
}
