package com.migratorydata.extensions.limits;

import com.migratorydata.extensions.authorization.AuthorizationListener;
import com.migratorydata.extensions.authorization.Producer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static com.migratorydata.extensions.util.Util.toEpochNanos;

public class TopicStatsListener implements MigratoryDataTopicStatsListener {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private AuthorizationListener authorizationListener;

    private final Producer producer;
    private final String serverName;
    private final String topicStats;

    public TopicStatsListener() {
        log("@@@@@ TOPICSTATS EXTENSION @@@@");

        this.authorizationListener = AuthorizationListener.getInstance();
        this.producer = this.authorizationListener.getProducer();

        this.serverName = AuthorizationListener.serverName;
        this.topicStats = AuthorizationListener.topicStats;
    }

    @Override
    public void onMessagesLimit(Map<String, Integer> map) {
        if (map.size() > 0) {
            JSONObject metricStats = new JSONObject();
            metricStats.put("op", "messages");
            metricStats.put("server", serverName);
            metricStats.put("timestamp", toEpochNanos(Instant.now()));

            JSONArray metrics = new JSONArray();
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                JSONObject metric = new JSONObject();
                metric.put("topic", entry.getKey());
                metric.put("value", entry.getValue());
                metrics.put(metric);
            }
            metricStats.put("metrics", metrics);

            producer.write(topicStats, metricStats.toString().getBytes(), serverName);
        }
    }

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "ACCESS", info));
    }

}
