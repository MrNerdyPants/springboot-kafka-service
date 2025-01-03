package com.dust.corp.kafka.sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class KafkaSyncProducer {
    private KafkaProducer<String, String> producer;
    private final ConcurrentHashMap<String, CountDownLatch> latchMap;
    private final ConcurrentHashMap<String, String> responseMap;

    public KafkaSyncProducer(ConcurrentHashMap<String, CountDownLatch> latchMap, ConcurrentHashMap<String, String> responseMap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
        this.latchMap = latchMap;
        this.responseMap = responseMap;
    }

    public String sendMessage(String topic, String value) throws InterruptedException {
        String correlationId = UUID.randomUUID().toString(); // Generate a unique ID
        CountDownLatch latch = new CountDownLatch(1); // Create a latch for this correlation ID

        latchMap.put(correlationId, latch); // Add the latch to the map

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, correlationId, value);

        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception == null) {
                System.out.println("Message sent successfully with Correlation ID: " + correlationId);
            } else {
                exception.printStackTrace();
            }
        });

        System.out.println("Waiting for consumer notification for Correlation ID: " + correlationId);
        latch.await(); // Wait for the consumer to notify

        String response = responseMap.remove(correlationId); // Retrieve and remove the response
        latchMap.remove(correlationId); // Cleanup the latch
        return response;
    }

    public void close() {
        producer.close();
    }
}

