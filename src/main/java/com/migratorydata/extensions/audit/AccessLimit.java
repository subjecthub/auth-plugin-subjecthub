package com.migratorydata.extensions.audit;

import com.migratorydata.extensions.authorization.AuthorizationListener;
import com.migratorydata.extensions.authorization.Producer;
import com.migratorydata.extensions.util.Metric;
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

import static com.migratorydata.extensions.util.Util.getKeyElements;
import static com.migratorydata.extensions.util.Util.toEpochNanos;

public class AccessLimit implements MigratoryDataAccessListener {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private AuthorizationListener authorizationListener;

    private Map<String, Metric> applicationsToConnections = new HashMap<>(); // app => connections

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

            String[] topicAndApplication = getKeyElements(connectEvent.getToken());

            if (topicAndApplication == null) {
                return;
            }

            String topic = topicAndApplication[0];
            String application = topicAndApplication[1];

            Metric metric = applicationsToConnections.get(application);
            if (metric == null) {
                applicationsToConnections.put(application, new Metric(topic, application, 1));
            } else {
                applicationsToConnections.put(application, new Metric(topic, application, metric.value + 1));
            }
        });
    }

    @Override
    public void onDisconnect(DisconnectEvent disconnectEvent) {
        executor.execute(() -> {
            log("onDisconnect = " + disconnectEvent);

            ConnectEvent castConnectEvent = (ConnectEvent) disconnectEvent;

            String[] topicAndApplication = getKeyElements(castConnectEvent.getToken());
            if (topicAndApplication == null) {
                return;
            }

            String topic = topicAndApplication[0];
            String application = topicAndApplication[1];

            Metric metric = applicationsToConnections.get(application);
            if (metric != null && metric.value > 0) {
                applicationsToConnections.put(application, new Metric(topic, application, metric.value - 1));
            }
        });
    }

    private void update() {
        JSONObject connectionsStats = new JSONObject();
        connectionsStats.put("op", "connections");
        connectionsStats.put("server", serverName);
        connectionsStats.put("timestamp", toEpochNanos(Instant.now()));

        JSONArray metrics = new JSONArray();
        for (Map.Entry<String, Metric> entry : applicationsToConnections.entrySet()) {
            JSONObject metric = new JSONObject();
            metric.put("topic", entry.getValue().topic);
            metric.put("application", entry.getKey());
            metric.put("value", entry.getValue().value);
            metrics.put(metric);
        }
        connectionsStats.put("metrics", metrics);

        producer.write(topicStats, connectionsStats.toString().getBytes());
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
