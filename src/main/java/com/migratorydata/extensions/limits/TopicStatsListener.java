package com.migratorydata.extensions.limits;

import com.migratorydata.extensions.authorization.AuthorizationListener;
import com.migratorydata.extensions.authorization.Producer;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

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
            JSONObject connectionsStats = new JSONObject();
            connectionsStats.put("op", "messages");
            connectionsStats.put("server", serverName);
            connectionsStats.put("messages", new JSONObject(map));

            producer.write(topicStats, connectionsStats.toString().getBytes(), serverName);
        }
    }

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "ACCESS", info));
    }

}
