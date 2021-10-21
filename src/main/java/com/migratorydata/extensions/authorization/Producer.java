package com.migratorydata.extensions.authorization;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    private final KafkaProducer<String, byte[]> producer;

    public Producer(String kafkaCluster) {
        Properties props = new Properties();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster);

        producer = new KafkaProducer<>(props);
    }

    public void write(String topic, byte[] data, String key) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, data);
        producer.send(record);
    }
}
