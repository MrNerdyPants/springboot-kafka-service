package com.dust.corp.kafka.sample.duplicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaSyncConsumer2 {
    private KafkaConsumer<String, String> consumer;
    private KafkaSyncProducer2 kafkaSyncProducer2;
//    private final ConcurrentHashMap<String, CountDownLatch> latchMap;
//    private final ConcurrentHashMap<String, String> responseMap;

    public KafkaSyncConsumer2() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "notificationGroup");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList("notification_topic"));
//        this.latchMap = latchMap;
//        this.responseMap = responseMap;
    }

    public void listenForResponses() {
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String correlationId = record.key(); // Extract the correlation ID
                    String response = record.value();

                    System.out.println("Consumer received: " + response + " with Correlation ID: " + correlationId);

                    if (kafkaSyncProducer2 == null)
                        kafkaSyncProducer2 = new KafkaSyncProducer2();

                    try {
                        kafkaSyncProducer2.sendMessage(correlationId, "notification_topic2", response);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            }
        }).start();
    }

    public void close() {
        consumer.close();
    }
}

