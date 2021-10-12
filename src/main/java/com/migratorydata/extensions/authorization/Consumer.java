package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.util.Util;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final KafkaConsumer<String, byte[]> consumer;
    private final Thread thread;

    private final Properties props;
    private final List<String> topicList;
    private final AuthorizationManager authorizationManager;

    public Consumer(String kafkaCluster, String topics, AuthorizationManager authorizationManager) {
        props = new Properties();
        this.authorizationManager = authorizationManager;
        // generate unique group id

        String groupId = "AuthorizationKafkaConsumer-" + String.valueOf(UUID.randomUUID());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        topicList = Util.getKafkaTopics(topics);

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

        consumer.subscribe(topicList);

        try {
            while (!closed.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, byte[]> record : records) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{}-{}-{} key={}, value={}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    //System.out.printf("%s-%d-%d, key = %s, value = %s --- %d %n", record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.timestamp());


                    if (record.topic().equals("entitlement")) {
                        authorizationManager.onMessage(new String(record.value()));
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
