package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.audit.PublishLimit;
import com.migratorydata.extensions.presence.MigratoryDataPresenceListener;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class AuthorizationListener implements MigratoryDataEntitlementListener {

    private static final String authFileName = "authorization.conf";

    private static String serviceToken = "some-token";
    private static String serviceSubject = "/migratory/secret";
    private static String cluster = "192.168.1.104:8800";

    private static String dbConnector = "mysql";
    private static String dbIp;
    private static String dbName;
    private static String user;
    private static String password;

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
            serviceToken = prop.getProperty("service.token");
            serviceSubject = prop.getProperty("service.subject");
            cluster = prop.getProperty("service.cluster");

            dbConnector = prop.getProperty("db.connector");
            dbIp = prop.getProperty("db.ip");
            dbName = prop.getProperty("db.name");
            user = prop.getProperty("db.user");
            password = prop.getProperty("db.password");

            serverName = System.getProperty("com.migratorydata.extensions.authorization.serverName", "server1");
            nodeIndex = Integer.valueOf(System.getProperty("com.migratorydata.extensions.authorization.index", "0"));
        }
    }

    private AuthorizationManager authorizationManager;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    public static AuthorizationListener INSTANCE;

    public AuthorizationListener() {
        System.out.println("@@@@@@@ CREATE AUTHORIZATION EXTENSION LISTENER INSTANCE @@@@@@");
        logConfig();

        try {
            authorizationManager = new AuthorizationManager(cluster, serviceToken, serviceSubject,
                    dbConnector, dbIp, dbName, user, password, serverName, nodeIndex == 0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        Thread loop = new Thread(authorizationManager);
        loop.setDaemon(true);
        loop.setName("SecretKeyManagerLoop");
        loop.start();

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

    public void updatePublishLimit(Map<String, PublishLimit.PublishCount> copyPublishLimit) {
        authorizationManager.updatePublishLimit(copyPublishLimit);
    }

    public void updateAccessLimit(Map<String, Integer> copyAppCountClients) {
        authorizationManager.updateAccessLimit(copyAppCountClients);
    }

    public void onConnectorRequest(MigratoryDataPresenceListener.Message message) {
        authorizationManager.onConnectorMessage(message);
    }

    private void logConfig() {
        System.out.println("@@@@@@@ AUTHORIZATION EXTENSION LISTENER CONFIG:");
        System.out.println("\t\t\tserverName=" + serverName);
        System.out.println("\t\t\tservice.token=" + serviceToken);
        System.out.println("\t\t\tservice.subject=" + serviceSubject);
        System.out.println("\t\t\tservice.cluster=" + cluster);

        System.out.println("\t\t\tdb.connector=" + dbConnector);
        System.out.println("\t\t\tdb.ip=" + dbIp);
        System.out.println("\t\t\tdb.name=" + dbName);
        System.out.println("\t\t\tdb.user=" + user);
        System.out.println("\t\t\tdb.password=" + password);
    }

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "AUTHORIZATION", info));
    }
}
