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

    private static String dbIp;
    private static String dbName;
    private static String user;
    private static String password;

    static {
        Properties prop = null;

        try (InputStream input = new FileInputStream("./extensions/" + authFileName)) {
            System.out.println("load from ./extensions/" + authFileName);
            prop = new Properties();
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();

            try (InputStream input = new FileInputStream("./" + authFileName)) {
                System.out.println("load from ./" + authFileName);
                prop = new Properties();
                prop.load(input);
            } catch (IOException exx) {
                exx.printStackTrace();

                try (InputStream input = SecretKeyManager.class.getClassLoader().getResourceAsStream(authFileName)) {
                    System.out.println("load from resources = " + authFileName);
                    prop = new Properties();
                    prop.load(input);
                } catch (IOException exxx) {
                    exxx.printStackTrace();
                }
            }
        }

        if (prop != null) {
            token = prop.getProperty("service.token");
            serviceSubject = prop.getProperty("service.subject");
            cluster = prop.getProperty("service.cluster");

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
                    Map<String, Key> subjectToKey = mySqlAccess.readDataBase(dbIp, dbName, user, password);
                    if (subjectToKey.size() > 0) {
                        keys.putAll(subjectToKey);
                    }
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

    public Key getKey(String subject) {
        return keys.get(subject);
    }

    @Override
    public void onMessage(MigratoryDataMessage migratoryDataMessage) {
        switch (migratoryDataMessage.getMessageType()) {
            case UPDATE:
            case RECOVERED:
                queue.offer(() -> {
                    JSONObject jsonObject = new JSONObject(new String(migratoryDataMessage.getContent()));
                    String prefix = (String) jsonObject.get("prefix");
                    String publishKey = (String) jsonObject.get("publish_key");
                    String subscribeKey = (String) jsonObject.get("subscribe_key");
                    String pubSubKey = (String) jsonObject.get("pub_sub_key");
                    String type = (String) jsonObject.get("type");
                    String operation = (String) jsonObject.get("operation");

                    System.out.println("Update keys with:");
                    System.out.println("prefix=" + prefix + ", type=" + type + ", publish_key=" + publishKey + ", subscribe_key=" + subscribeKey + ", pub_sub_key=" + pubSubKey);

                    keys.put(prefix, new Key("private".equals(type), publishKey, subscribeKey, pubSubKey));
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
