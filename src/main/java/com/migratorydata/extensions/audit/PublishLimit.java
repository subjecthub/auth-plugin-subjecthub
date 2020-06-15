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

    private final Map<String, PublishCount> publishLimit = new HashMap<>(); // subjecthubId => PublishCount

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

            PublishCount publishCount = publishLimit.get(subjecthubId);
            if (publishCount == null) {
                publishCount = new PublishCount();
                publishLimit.put(subjecthubId, publishCount);
            }
            publishCount.current++;
        });
    }

    private void update() {
        long currentTime = System.currentTimeMillis();

        // update authorization extension
        Map<String, PublishCount> copyPublishLimit = new HashMap<>();
        for (Map.Entry<String, PublishCount> entry : publishLimit.entrySet()) {
            PublishCount currentPublishCount = entry.getValue();

            PublishCount copyCurrentPublishCount = new PublishCount(currentPublishCount);
            copyPublishLimit.put(entry.getKey(), copyCurrentPublishCount);

            currentPublishCount.previous = currentPublishCount.current;
        }
        authorizationListener.updatePublishLimit(copyPublishLimit);

        // every hour reset the publish limit
        if (currentTime - startCountPublishLimit >= 3600000) { // one hour
            startCountPublishLimit = currentTime;
            publishLimit.clear();

            authorizationListener.updatePublishLimit(new HashMap<>(publishLimit));
        }
    }

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "MESSAGE", info));
    }

    public class PublishCount {
        public int previous;
        public int current;

        public PublishCount() {
            this.previous = 0;
            this.current = 0;
        }

        public PublishCount(PublishCount c) {
            this.previous = c.previous;
            this.current = c.current;
        }
    }
}
