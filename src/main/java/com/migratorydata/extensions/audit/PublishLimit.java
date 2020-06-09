package com.migratorydata.extensions.audit;

import com.migratorydata.extensions.authorization.AuthorizationListener;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.migratorydata.extensions.util.Util.getSubjecthubId;

public class PublishLimit implements MigratoryDataMessageListener {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private AuthorizationListener authorizationListener;

    private final Map<String, Integer> publishLimit = new HashMap<>(); // subjecthubId => PublishCount

    private long startCountPublishLimit = System.currentTimeMillis();

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public PublishLimit() {
        log("@@@@@@@ AUDIT PUBLISH EXTENSION @@@@@@@");
        authorizationListener = AuthorizationListener.getInstance();

        executor.scheduleAtFixedRate(() -> {
            update();
        }, 0, 5, TimeUnit.SECONDS);
    }

    // Audit.Publish
    @Override
    public void onMessage(MessageEvent messageEvent) {
        executor.execute(() -> {
            log ("onMessage = " + messageEvent);

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
        });
    }

    private void update() {
        long currentTime = System.currentTimeMillis();

        // every hour reset the publish limit
        if (currentTime - startCountPublishLimit >= 3600000) { // one hour
            startCountPublishLimit = currentTime;
            publishLimit.clear();
        }

        // update authorization extension
        Map<String, Integer> copyPublishLimit = new HashMap<>(publishLimit);
        authorizationListener.updatePublishLimit(copyPublishLimit);
    }

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MESSAGE", info));
    }
}
