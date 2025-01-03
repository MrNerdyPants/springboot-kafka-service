package com.dust.corp.kafka.sample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class KafkaSyncConsumer {
    private KafkaConsumer<String, String> consumer;
    private final ConcurrentHashMap<String, CountDownLatch> latchMap;
    private final ConcurrentHashMap<String, String> responseMap;

    public KafkaSyncConsumer(ConcurrentHashMap<String, CountDownLatch> latchMap, ConcurrentHashMap<String, String> responseMap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "notificationGroup");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList("notification_topic"));
        this.latchMap = latchMap;
        this.responseMap = responseMap;
    }

    public void listenForResponses() {
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String correlationId = record.key(); // Extract the correlation ID
                    String response = record.value();

                    System.out.println("Consumer received: " + response + " with Correlation ID: " + correlationId);

                    if (latchMap.containsKey(correlationId)) {
                        responseMap.put(correlationId, response); // Save the response
                        latchMap.get(correlationId).countDown(); // Notify the waiting producer
                    } else {
                        System.err.println("No producer is waiting for Correlation ID: " + correlationId);
                    }
                }
            }
        }).start();
    }

    public void close() {
        consumer.close();
    }
}

