package com.migratorydata.extensions.authorization;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class AuthorizationListener implements MigratoryDataEntitlementListener {

    private static final String authFileName = "authorization.conf";

    private static String kafkaCluster = "localhost:9092";
    private static String topics = "entitlement";

    private static String serverName = "server1";
    private static int nodeIndex = 0;

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
            try (InputStream input = AuthorizationManager.class.getClassLoader().getResourceAsStream(authFileName)) {
                System.out.println("load from resources = " + authFileName);
                prop = new Properties();
                prop.load(input);
                loadConfig = true;
            } catch (IOException exxx) {
                exxx.printStackTrace();
            }
        }

        if (loadConfig) {
            kafkaCluster = prop.getProperty("bootstrap.servers");
            topics = prop.getProperty("topics");

            serverName = System.getProperty("com.migratorydata.extensions.authorization.serverName", "server1");
            nodeIndex = Integer.valueOf(System.getProperty("com.migratorydata.extensions.authorization.index", "0"));
        }
    }

    private AuthorizationManager authorizationManager;
    private Consumer consumer;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    public static AuthorizationListener INSTANCE;

    public AuthorizationListener() {
        System.out.println("@@@@@@@ CREATE AUTHORIZATION EXTENSION LISTENER INSTANCE @@@@@@");
        logConfig();

        try {
            authorizationManager = new AuthorizationManager();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        Thread loop = new Thread(authorizationManager);
        loop.setDaemon(true);
        loop.setName("SecretKeyManagerLoop");
        loop.start();

        consumer = new Consumer(kafkaCluster, topics, authorizationManager);
        consumer.begin();

        synchronized (AuthorizationListener.class) {
            INSTANCE = this;
        }
    }

    public AuthorizationListener(AuthorizationManager authorizationManager) {
        this.authorizationManager = authorizationManager;
    }

    synchronized public static AuthorizationListener getInstance() {
        return INSTANCE;
    }

    @Override
    public void onSubscribe(MigratoryDataSubscribeRequest migratoryDataSubscribeRequest) {
        log("SUBSCRIBE=" + migratoryDataSubscribeRequest);

        authorizationManager.handleSubscribeCheck(migratoryDataSubscribeRequest);
    }

    @Override
    public void onPublish(MigratoryDataPublishRequest migratoryDataPublishRequest) {
        log("PUBLISH=" + migratoryDataPublishRequest);

        authorizationManager.handlePublishCheck(migratoryDataPublishRequest);
    }

//    public void updatePublishLimit(Map<String, PublishLimit.PublishCount> copyPublishLimit) {
//        authorizationManager.updatePublishLimit(copyPublishLimit);
//    }
//
//    public void updateAccessLimit(Map<String, Integer> copyAppCountClients) {
//        authorizationManager.updateAccessLimit(copyAppCountClients);
//    }

//    public void onConnectorRequest(MigratoryDataPresenceListener.Message message) {
//        authorizationManager.onConnectorMessage(message);
//    }

    private void logConfig() {
        System.out.println("@@@@@@@ AUTHORIZATION EXTENSION LISTENER CONFIG:");
        System.out.println("\t\t\tkafkaCluster=" + serverName);
        System.out.println("\t\t\ttopics=" + topics);
    }

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "AUTHORIZATION", info));
    }
}
