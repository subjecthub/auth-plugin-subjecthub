package com.migratorydata.extensions.authorization;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    private final KafkaProducer<String, byte[]> producer;

    public Producer(Properties p) {
        Properties producerProps = new Properties();
        for (String pp : p.stringPropertyNames()) {
            producerProps.put(pp, p.get(pp));
        }

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<>(producerProps);
    }

    public void write(String topic, byte[] data, String key) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, data);
        producer.send(record);
    }
}
