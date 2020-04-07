package com.migratorydata.extensions.authorization;

import java.util.List;

public class DefaultAuthorizationListener implements MigratoryDataEntitlementListener {

    private SecretKeyManager secretKeyManager = new SecretKeyManager();

	public DefaultAuthorizationListener() {
	    System.out.println("@@@@@@@ CREATE AUTHORIZATION LISTENER INSTANCE @@@@@@");
        Thread loop = new Thread(secretKeyManager);
        loop.setDaemon(true);
        loop.setName("SecretKeyManagerLoop");
        loop.start();
    }

	@Override
	public void onSubscribe(MigratoryDataSubscribeRequest migratoryDataSubscribeRequest) {
		System.out.println("Got subscribe request=" + migratoryDataSubscribeRequest);
        secretKeyManager.offer(() -> {
            String token = migratoryDataSubscribeRequest.getClientCredentials().getToken();
            List<String> subjects = migratoryDataSubscribeRequest.getSubjects();
            for (String subject : subjects) {
                // auth service client
                if (subject.equals(secretKeyManager.getServiceSubject())) {
                    if (token.equals(secretKeyManager.getServiceToken())) {
                        migratoryDataSubscribeRequest.setAllowed(subject, true);
                    } else {
                        migratoryDataSubscribeRequest.setAllowed(subject, false);
                    }
                } else {
                    boolean allowSubscribe = false;
                    if (secretKeyManager.isPublicSubject(subject)) {
                        allowSubscribe = true;
                    } else {
                        String[] appIdAndSecret = getAppIdAndSecret(token);
                        if (appIdAndSecret != null) {
                            Key key = secretKeyManager.getAppKeys(appIdAndSecret[0]);
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

	@Override
	public void onPublish(MigratoryDataPublishRequest migratoryDataPublishRequest) {
		System.out.println("Got publish request=" + migratoryDataPublishRequest);

		if (migratoryDataPublishRequest.getSubject().equals(secretKeyManager.getServiceSubject())) {
		    // allow publish on service subject with service.token
            if (migratoryDataPublishRequest.getClientCredentials().getToken().equals(secretKeyManager.getServiceToken())) {
                migratoryDataPublishRequest.setAllowed(true);
            }

            migratoryDataPublishRequest.sendResponse();
        } else {
            secretKeyManager.offer(() -> {
                String subject = migratoryDataPublishRequest.getSubject();
                String token = migratoryDataPublishRequest.getClientCredentials().getToken();

                String[] appIdAndSecret = getAppIdAndSecret(token);
                if (appIdAndSecret != null) {
                    Key key = secretKeyManager.getAppKeys(appIdAndSecret[0]);
                    if (key != null && key.checkPublish(appIdAndSecret[1])) {
                        migratoryDataPublishRequest.setAllowed(true);
                    } else {
                        migratoryDataPublishRequest.setAllowed(false);
                    }
                }

                migratoryDataPublishRequest.sendResponse();
            });
        }
	}

	public static String[] getAppIdAndSecret(String token) {
	    String[] elements = token.split(":");

	    if (elements.length == 2) {
	        return elements;
        }

	    return null;
    }

}
