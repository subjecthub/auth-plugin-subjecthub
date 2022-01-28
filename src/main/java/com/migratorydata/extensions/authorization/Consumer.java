package com.migratorydata.extensions.authorization;

import com.migratorydata.client.MigratoryDataClient;
import com.migratorydata.client.MigratoryDataListener;
import com.migratorydata.client.MigratoryDataMessage;
import com.migratorydata.extensions.util.Metric;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer implements MigratoryDataListener {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private MigratoryDataClient client;

    private final Properties props;
    private final List<String> topicList;
    private final AuthorizationManager authorizationManager;
    private final String topicStats;
    private final String topicEntitlement;

    public Consumer(Properties p, String topicEntitlement, String topicStats, AuthorizationManager authorizationManager) {
        this.props = new Properties();
        for (String pp : p.stringPropertyNames()) {
            this.props.put(pp, p.get(pp));
        }
        this.authorizationManager = authorizationManager;

        client = new MigratoryDataClient();
        client.setServers(p.getProperty("push.servers").split(","));
        client.setEntitlementToken(p.getProperty("token"));
        client.setListener(this);

        this.topicEntitlement = topicEntitlement;
        this.topicStats = topicStats;

        topicList = Arrays.asList(topicEntitlement, topicStats);

        client.subscribe(topicList);
    }

    public void begin() {
        client.connect();
    }

    public void end() {
        client.disconnect();
        closed.set(true);
    }

    @Override
    public void onMessage(MigratoryDataMessage m) {
        if (logger.isDebugEnabled()) {
            logger.debug("{}", m);
        }

        if (m.getMessageType() == MigratoryDataMessage.MessageType.SNAPSHOT) {
            return;
        }

        if (m.getSubject().equals(topicEntitlement)) {
            authorizationManager.onMessage(new String(m.getContent()));
        } else {
            JSONObject result = new JSONObject(new String(m.getContent()));
            String op = result.getString("op");
            String serverName = result.getString("server");
            JSONArray metrics = result.getJSONArray("metrics");
            Map<String, Metric> metricsMap = new HashMap<>();
            for (int i = 0; i < metrics.length(); i++) {
                String topicName = null;
                try {
                    topicName = metrics.getJSONObject(i).getString("topic");
                } catch (Exception e) {
                }
                String application = metrics.getJSONObject(i).getString("application");
                int value = metrics.getJSONObject(i).getInt("value");
                metricsMap.put(application, new Metric(topicName, application, value));
            }

            if ("connections".equals(op)) {
                authorizationManager.updateConnections(metricsMap, serverName);
            } else if ("messages".equals(op)) {
                authorizationManager.updateMessages(metricsMap, serverName);
            }
        }
    }

    @Override
    public void onStatus(String s, String s1) {

    }
}
