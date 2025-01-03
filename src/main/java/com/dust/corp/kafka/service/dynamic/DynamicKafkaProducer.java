package com.dust.corp.kafka.service.dynamic;

import com.dust.corp.kafka.service.sync.KafkaSyncProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@Service
public class DynamicKafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaSyncProducer kafkaSyncProducer;


    private final ConcurrentHashMap<String, CountDownLatch> latchMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> responseMap = new ConcurrentHashMap<>();


    public void sendMessage(String topicName, String message) throws InterruptedException {


        String correlationId = UUID.randomUUID().toString(); // Generate a unique ID
        CountDownLatch latch = new CountDownLatch(1); // Create a latch for this correlation ID

        latchMap.put(correlationId, latch); // Add the latch to the map

        kafkaSyncProducer.sendMessage(topicName, message);
//        CompletableFuture<SendResult<String, String>> future =
//                kafkaTemplate.send(topicName, message);
//
//        future.thenAccept(result ->
//                System.out.println("Message sent successfully to topic: " + topicName +
//                        " with offset: " + result.getRecordMetadata().offset())
//        ).exceptionally(ex -> {
//            System.err.println("Failed to send message to topic: " + topicName +
//                    " due to: " + ex.getMessage());
//            return null;
//        });
    }
}

