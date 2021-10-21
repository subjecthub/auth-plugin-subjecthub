package com.migratorydata.extensions.authorization;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class AuthorizationListener implements MigratoryDataEntitlementListener {

    private static final String authFileName = "authorization.conf";

    private static String kafkaCluster = "localhost:9092";

    public static String serverName = "server1";
    public static String topicEntitlement = "entitlement";
    public static String topicStats = "stats";

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
            try (InputStream input = new FileInputStream("/usr/share/migratorydata-ke/extensions/" + authFileName)) {
                System.out.println("load from /usr/share/migratorydata-ke/extensions/" + authFileName);
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
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        if (loadConfig == false) {
            try (InputStream input = AuthorizationManager.class.getClassLoader().getResourceAsStream(authFileName)) {
                System.out.println("load from resources = " + authFileName);
                prop = new Properties();
                prop.load(input);
                loadConfig = true;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        if (loadConfig) {
            kafkaCluster = prop.getProperty("bootstrap.servers");

            topicEntitlement = prop.getProperty("topic.entitlement");
            topicStats = prop.getProperty("topic.stats");

            serverName = UUID.randomUUID().toString();
        }
    }

    private AuthorizationManager authorizationManager;
    private Consumer consumer;
    private Producer producer;
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
        loop.setName("AuthorizationManager");
        loop.start();

        consumer = new Consumer(kafkaCluster, topicEntitlement, topicStats, authorizationManager);
        consumer.begin();

        producer = new Producer(kafkaCluster);

        synchronized (AuthorizationListener.class) {
            INSTANCE = this;
        }
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

    private void logConfig() {
        System.out.println("@@@@@@@ AUTHORIZATION EXTENSION LISTENER CONFIG:");
        System.out.println("\t\t\tkafkaCluster=" + kafkaCluster);
        System.out.println("\t\t\tserverName=" + serverName);
        System.out.println("\t\t\ttopics=" + topicEntitlement + ", " + topicStats);
    }

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "AUTHORIZATION", info));
    }

    public Producer getProducer() {
        return producer;
    }
}
