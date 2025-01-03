package com.dust.corp.kafka.sample;

import com.dust.corp.kafka.sample.duplicate.KafkaSyncConsumer2;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@Service
public class KafkaSyncService {
    public void startProcess() throws InterruptedException {
        KafkaSyncConsumer2 consumer2 = new KafkaSyncConsumer2();
        consumer2.listenForResponses();

        ConcurrentHashMap<String, CountDownLatch> latchMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> responseMap = new ConcurrentHashMap<>();

        KafkaSyncConsumer consumer = new KafkaSyncConsumer(latchMap, responseMap);
        KafkaSyncProducer producer = new KafkaSyncProducer(latchMap, responseMap);

        consumer.listenForResponses(); // Start the consumer thread

        System.out.println("\n\n\n\n");

        System.out.println("Sending message for User 1...");
        String response1 = producer.sendMessage("notification_topic", "Hello, Kafka User 1! + "+ UUID.randomUUID().toString());
        System.out.println("Response for User 1: " + response1);

        System.out.println("\n\n\n\n");

        System.out.println("Sending message for User 2...");
        String response2 = producer.sendMessage("notification_topic", "Hello, Kafka User 2! + "+ UUID.randomUUID().toString());
        System.out.println("Response for User 2: " + response2);

//        producer.close();
//        consumer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaSyncService kafkaSyncService = new KafkaSyncService();
        kafkaSyncService.startProcess();
    }
}

