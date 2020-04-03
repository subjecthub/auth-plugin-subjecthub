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
                    boolean allowSubscribe = false;
                    String[] subjectHubIdAndGroup = getSubjectHubIdAndGroup(subject);
                    if (subjectHubIdAndGroup != null) {
                        if (secretKeyManager.isPublicSubject(subjectHubIdAndGroup[0], subjectHubIdAndGroup[1], subject)) {
                            allowSubscribe = true;
                        } else {
                            Key key = secretKeyManager.getGroupKeys(subjectHubIdAndGroup[0]);
                            if (key != null && key.checkSubscribe(subjectHubIdAndGroup[1], secretKey)) {
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
                String secretKey = migratoryDataPublishRequest.getClientCredentials().getToken();

                String[] subjectHubIdAndGroup = getSubjectHubIdAndGroup(subject);
                if (subjectHubIdAndGroup != null) {
                    Key key = secretKeyManager.getGroupKeys(subjectHubIdAndGroup[0]);
                    if (key != null && key.checkPublish(subjectHubIdAndGroup[1], secretKey)) {
                        migratoryDataPublishRequest.setAllowed(true);
                    } else {
                        migratoryDataPublishRequest.setAllowed(false);
                    }
                }

                migratoryDataPublishRequest.sendResponse();
            });
        }
	}

	public static String[] getSubjectHubIdAndGroup(String subject) {
	    String[] elements = subject.split("/");

	    if (elements.length >= 3) {
	        String[] result = new String[2];
	        result[0] = elements[1];
	        result[1] = elements[2];
	        return result;
        }

	    return null;
    }

}
