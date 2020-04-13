package com.migratorydata.extensions.authorization;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
        }
    }

    private AuthorizationManager authorizationManager;

    public static AuthorizationListener INSTANCE;

	public AuthorizationListener() {
	    System.out.println("@@@@@@@ CREATE AUTHORIZATION EXTENSION LISTENER INSTANCE @@@@@@");
        logConfig();

        authorizationManager = new AuthorizationManager(cluster, serviceToken, serviceSubject, dbConnector, dbIp, dbName,
                user, password, serverName);
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
		System.out.println("Got subscribe request=" + migratoryDataSubscribeRequest);

		authorizationManager.handleSubscribeCheck(migratoryDataSubscribeRequest);
	}

	@Override
	public void onPublish(MigratoryDataPublishRequest migratoryDataPublishRequest) {
		System.out.println("Got publish request=" + migratoryDataPublishRequest);

        authorizationManager.handlePublishCheck(migratoryDataPublishRequest);
	}

    public void updatePublishLimit(Map<String, Integer> copyPublishLimit) {
        authorizationManager.updatePublishLimit(copyPublishLimit);
	}

    public void updateAccessLimit(Map<String, Integer> copyAppCountClients) {
        authorizationManager.updateAccessLimit(copyAppCountClients);
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
}
