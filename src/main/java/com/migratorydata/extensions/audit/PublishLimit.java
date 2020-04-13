package com.migratorydata.extensions.audit;

import com.migratorydata.extensions.authorization.AuthorizationListener;

import java.util.HashMap;
import java.util.Map;

import static com.migratorydata.extensions.util.Util.getSubjecthubId;

public class PublishLimit implements MigratoryDataMessageListener {

    private AuthorizationListener authorizationListener;

    private final Map<String, Integer> publishLimit = new HashMap<>(); // subjecthubId => PublishCount

    private long startCountPublishLimit = System.currentTimeMillis();
    private long lastUpdateOfPublishLimit = System.currentTimeMillis();

    public PublishLimit() {
        System.out.println("@@@@@@@ AUDIT PUBLISH EXTENSION @@@@@@@");
        authorizationListener = AuthorizationListener.getInstance();
    }

    // Audit.Publish
    @Override
    public void onMessage(MessageEvent messageEvent) {
        System.out.println("onMessage = " + messageEvent);

        String subjecthubId = getSubjecthubId(messageEvent.getSubject());
        if (subjecthubId == null) {
            return;
        }

        Integer count = publishLimit.get(subjecthubId);
        if (count == null) {
            publishLimit.put(subjecthubId, Integer.valueOf(1));
        } else {
            publishLimit.put(subjecthubId, Integer.valueOf(count.intValue() + 1));
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime - lastUpdateOfPublishLimit >= 5000) { // 5 seconds

            // every hour reset the publish limit
            if (currentTime - startCountPublishLimit >= 3600000) { // one hour
                startCountPublishLimit = currentTime;
                publishLimit.clear();
            }

            // update authorization extension
            Map<String, Integer> copyPublishLimit = new HashMap<>(publishLimit);
            authorizationListener.updatePublishLimit(copyPublishLimit);
            lastUpdateOfPublishLimit = currentTime;
        }
    }
}
