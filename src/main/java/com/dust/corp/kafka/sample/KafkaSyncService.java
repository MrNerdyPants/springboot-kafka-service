package com.dust.corp.kafka.sample;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class KafkaSyncService {
    public static void main(String[] args) throws InterruptedException {
        ConcurrentHashMap<String, CountDownLatch> latchMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> responseMap = new ConcurrentHashMap<>();

        KafkaSyncConsumer consumer = new KafkaSyncConsumer(latchMap, responseMap);
        KafkaSyncProducer producer = new KafkaSyncProducer(latchMap, responseMap);

        consumer.listenForResponses(); // Start the consumer thread

        System.out.println("Sending message for User 1...");
        String response1 = producer.sendMessage("notification_topic", "Hello, Kafka User 1!");
        System.out.println("Response for User 1: " + response1);

        System.out.println("Sending message for User 2...");
        String response2 = producer.sendMessage("notification_topic", "Hello, Kafka User 2!");
        System.out.println("Response for User 2: " + response2);

//        producer.close();
//        consumer.close();
    }
}

