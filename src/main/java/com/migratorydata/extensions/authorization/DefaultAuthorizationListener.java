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
            String secretKey = migratoryDataSubscribeRequest.getClientCredentials().getToken();
            List<String> subjects = migratoryDataSubscribeRequest.getSubjects();
            for (String subject : subjects) {
                // auth service client
                if (subject.equals(secretKeyManager.getServiceSubject())) {
                    if (secretKey.equals(secretKeyManager.getServiceToken())) {
                        migratoryDataSubscribeRequest.setAllowed(subject, true);
                    } else {
                        migratoryDataSubscribeRequest.setAllowed(subject, false);
                    }
                } else {
                    if (secretKeyManager.isPublicSubject(subject)) {
                        migratoryDataSubscribeRequest.setAllowed(subject, true);
                    } else {
                        String group = getGroup(subject);
                        Key key = secretKeyManager.getGroupKeys(group);
                        if (key != null && key.checkSubscribe(secretKey)) {
                            migratoryDataSubscribeRequest.setAllowed(subject, true);
                        } else {
                            migratoryDataSubscribeRequest.setAllowed(subject, false);
                        }
                    }
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
                String secretKey = migratoryDataPublishRequest.getClientCredentials().getToken();

                String group = getGroup(subject);
                Key key = secretKeyManager.getGroupKeys(group);
                if (key != null && key.checkPublish(secretKey)) {
                    migratoryDataPublishRequest.setAllowed(true);
                } else {
                    migratoryDataPublishRequest.setAllowed(false);
                }

                migratoryDataPublishRequest.sendResponse();
            });
        }
	}

	private String getGroup(String subject) {
	    int endFirstSegment = subject.indexOf("/", 1);
	    if (endFirstSegment == -1) {
	        return subject;
        }
        return subject.substring(1, endFirstSegment);
    }

}
