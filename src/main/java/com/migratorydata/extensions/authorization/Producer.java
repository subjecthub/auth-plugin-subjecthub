package com.migratorydata.extensions.authorization;

import com.migratorydata.client.MigratoryDataClient;
import com.migratorydata.client.MigratoryDataListener;
import com.migratorydata.client.MigratoryDataMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer implements MigratoryDataListener {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private final MigratoryDataClient client;
    private int id;

    public Producer(Properties p) {
        Properties producerProps = new Properties();
        for (String pp : p.stringPropertyNames()) {
            producerProps.put(pp, p.get(pp));
        }

        client = new MigratoryDataClient();
        client.setServers(p.getProperty("push.servers").split(","));
        client.setEntitlementToken(p.getProperty("token"));
        client.setListener(this);

        client.connect();
    }

    public void write(String topic, byte[] data) {
        client.publish(new MigratoryDataMessage(topic, data, "closure-" + id++));
    }

    @Override
    public void onMessage(MigratoryDataMessage migratoryDataMessage) {

    }

    @Override
    public void onStatus(String status, String info) {
        logger.info("Extension-Producer-{}-{}", status, info);
    }
}
