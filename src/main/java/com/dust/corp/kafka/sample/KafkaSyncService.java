package com.dust.corp.kafka.sample;

import com.dust.corp.kafka.sample.duplicate.KafkaSyncConsumer2;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KafkaSyncService {

    KafkaSyncConsumer consumer;
    KafkaSyncConsumer2 consumer2;

    KafkaSyncProducer producer;

    ConcurrentHashMap<String, CountDownLatch> latchMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, String> responseMap = new ConcurrentHashMap<>();

    private static boolean isConsumerSubscribed = false;
    private static boolean isConsumer2Subscribed = false;

    public void startProcess() throws InterruptedException {

        if (!KafkaSyncService.isConsumer2Subscribed) {
            KafkaSyncConsumer2 consumer2 = new KafkaSyncConsumer2();
            consumer2.listenForResponses();
            KafkaSyncService.isConsumer2Subscribed = true;
        }

//        ConcurrentHashMap<String, CountDownLatch> latchMap = new ConcurrentHashMap<>();
//        ConcurrentHashMap<String, String> responseMap = new ConcurrentHashMap<>();


        if (!KafkaSyncService.isConsumerSubscribed) {
            consumer = new KafkaSyncConsumer(latchMap, responseMap);
            producer = new KafkaSyncProducer(latchMap, responseMap);

            consumer.listenForResponses(); // Start the consumer thread
            KafkaSyncService.isConsumerSubscribed = true;
        }

        ExecutorService executor = Executors.newFixedThreadPool(5); // Create a thread pool with 5 threads

        for (int i = 0; i < 10; i++) { // Loop to create and execute 10 tasks
            Runnable worker = new WorkerThread(i, "Hello, Kafka User " + i + "! + " + UUID.randomUUID().toString());
            executor.execute(worker); // Submit the worker to the executor
        }

        executor.shutdown(); // Shutdown the executor
        while (!executor.isTerminated()) {
            // Wait for all tasks to finish
        }

        System.out.println("All tasks are finished.");

//        System.out.println("\n\n\n\n");
//
//        System.out.println("Sending message for User 1...");
//        String response1 = producer.sendMessage("notification_topic", "Hello, Kafka User 1! + " + UUID.randomUUID().toString());
//        System.out.println("Response for User 1: " + response1);
//
//        System.out.println("\n\n\n\n");
//
//        System.out.println("Sending message for User 2...");
//        String response2 = producer.sendMessage("notification_topic", "Hello, Kafka User 2! + " + UUID.randomUUID().toString());
//        System.out.println("Response for User 2: " + response2);

//        producer.close();
//        consumer.close();
    }

    // WorkerThread class implementing Runnable
    class WorkerThread implements Runnable {

        private final String message;
        private final long threadId;


        public WorkerThread(long threadId, String message) {
            this.threadId = threadId;
            this.message = message;
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName() + " (Start) Message = " + message);
            processMessage(); // Simulate task processing
            System.out.println(Thread.currentThread().getName() + " (End)");
        }

        private void processMessage() {
            try {
                System.out.println("\n\n\n\n");

                System.out.println("Sending message for User " + threadId + " ...");
                String response1 = producer.sendMessage("notification_topic", message);
                System.out.println("Response for User 1: " + response1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaSyncService kafkaSyncService = new KafkaSyncService();
        kafkaSyncService.startProcess();
    }
}

