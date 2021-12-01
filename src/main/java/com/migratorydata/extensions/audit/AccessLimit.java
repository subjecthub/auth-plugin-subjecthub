package com.migratorydata.extensions.audit;

import com.migratorydata.extensions.authorization.AuthorizationListener;
import com.migratorydata.extensions.authorization.Producer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.migratorydata.extensions.util.Util.toEpochNanos;

public class AccessLimit implements MigratoryDataAccessListener {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private AuthorizationListener authorizationListener;

    private Map<String, Integer> topicsToConnections = new HashMap<>(); // topic => connections

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final String serverName;
    private final Producer producer;
    private final String topicStats;

    public AccessLimit() {
        log("@@@@@ AUDIT ACCESS EXTENSION @@@@");

        this.authorizationListener = AuthorizationListener.getInstance();
        this.producer = this.authorizationListener.getProducer();

        this.serverName = AuthorizationListener.serverName;
        this.topicStats = AuthorizationListener.topicStats;

        this.executor.scheduleAtFixedRate(() -> {
            update();
        }, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void onConnect(ConnectEvent connectEvent) {
        executor.execute(() -> {
            log("onConnect = " + connectEvent);

            String topic = getTopic(connectEvent.getToken());

            if (topic == null) {
                return;
            }

            Integer count = topicsToConnections.get(topic);
            if (count == null) {
                topicsToConnections.put(topic, Integer.valueOf(1));
            } else {
                topicsToConnections.put(topic, Integer.valueOf(count.intValue() + 1));
            }
        });
    }

    @Override
    public void onDisconnect(DisconnectEvent disconnectEvent) {
        executor.execute(() -> {
            log("onDisconnect = " + disconnectEvent);

            ConnectEvent castConnectEvent = (ConnectEvent) disconnectEvent;

            String topic = getTopic(castConnectEvent.getToken());
            if (topic == null) {
                return;
            }

            Integer count = topicsToConnections.get(topic);
            if (count != null && count.intValue() > 0) {
                topicsToConnections.put(topic, Integer.valueOf(count.intValue() - 1));
            }
        });
    }

    private void update() {
        JSONObject connectionsStats = new JSONObject();
        connectionsStats.put("op", "connections");
        connectionsStats.put("server", serverName);
        connectionsStats.put("timestamp", toEpochNanos(Instant.now()));

        JSONArray metrics = new JSONArray();
        for (Map.Entry<String, Integer> entry : topicsToConnections.entrySet()) {
            JSONObject metric = new JSONObject();
            metric.put("topic", entry.getKey());
            metric.put("value", entry.getValue());
            metrics.put(metric);
        }
        connectionsStats.put("metrics", metrics);

        producer.write(topicStats, connectionsStats.toString().getBytes(), serverName);
    }

    private String getTopic(String token) {
        if (token == null) {
            return null;
        }

        String[] elements = token.split(":");
        if (elements.length < 3) {
            return null;
        }

        return elements[0];
    }

    @Override
    public void onSubscribe(SubscribeEvent subscribeEvent) {
    }

    @Override
    public void onSubscribeWithHistory(SubscribeWithHistoryEvent subscribeWithHistoryEvent) {
    }

    @Override
    public void onSubscribeWithRecovery(SubscribeWithRecoveryEvent subscribeWithRecoveryEvent) {
    }

    @Override
    public void onUnsubscribe(UnsubscribeEvent unsubscribeEvent) {
    }

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "ACCESS", info));
    }
}
