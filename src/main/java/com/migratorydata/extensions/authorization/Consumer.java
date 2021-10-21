package com.migratorydata.extensions.authorization;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final KafkaConsumer<String, byte[]> consumer;
    private final Thread thread;

    private final Properties props;
    private final List<String> topicList;
    private final AuthorizationManager authorizationManager;
    private final String topicStats;
    private final String topicEntitlement;

    public Consumer(String kafkaCluster, String topicEntitlement, String topicStats, AuthorizationManager authorizationManager) {
        props = new Properties();
        this.authorizationManager = authorizationManager;
        // generate unique group id

        String groupId = "AuthorizationKafkaConsumer-" + String.valueOf(UUID.randomUUID());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        this.topicEntitlement = topicEntitlement;
        this.topicStats = topicStats;

        topicList = Arrays.asList(topicEntitlement, topicStats);

        consumer = new KafkaConsumer<String, byte[]>(props);

        this.thread = new Thread(this);
        this.thread.setName("KafkaAgentConsumer-" + thread.getId());
        this.thread.setDaemon(true);
    }

    public void begin() {
        thread.start();
    }

    public void end() {
        closed.set(true);
        consumer.wakeup();
    }

    @Override
    public void run() {

        ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition p : partitions) {
                    if (p.topic().equals(topicEntitlement)) {
                        consumer.seekToBeginning(Collections.singleton(p));
                    } else {
                        consumer.seekToEnd(Collections.singleton(p));
                    }
                }
            }
        };

        consumer.subscribe(topicList, rebalanceListener);

        try {
            while (!closed.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, byte[]> record : records) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{}-{}-{} key={}, value={}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    //System.out.printf("%s-%d-%d, key = %s, value = %s --- %d %n", record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.timestamp());


                    if (record.topic().equals(topicEntitlement)) {
                        authorizationManager.onMessage(new String(record.value()));
                    } else {
                        JSONObject result = new JSONObject(new String(record.value()));
                        String op = result.getString("op");
                        String serverName = result.getString("server");
                        if ("connections".equals(op)) {
                            Map connections = result.getJSONObject("connections").toMap();
                            authorizationManager.updateConnections(connections, serverName);
                        } else if ("messages".equals(op)) {
                            Map messages = result.getJSONObject("messages").toMap();
                            authorizationManager.updateMessages(messages, serverName);
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }

    }
}
