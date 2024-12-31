package com.dust.corp.kafka.service.dynamic;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class DynamicKafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    public void sendMessage(String topicName, String message) {
//        kafkaTemplate.send(topicName, message).
//                .addCallback(
//                        success -> System.out.println("Message sent to topic: " + topicName),
//                        failure -> System.err.println("Failed to send message: " + failure.getMessage())
//                );

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);

        future.thenAccept(result ->
                System.out.println("Message sent successfully to topic: " + topicName +
                        " with offset: " + result.getRecordMetadata().offset())
        ).exceptionally(ex -> {
            System.err.println("Failed to send message to topic: " + topicName +
                    " due to: " + ex.getMessage());
            return null;
        });
    }
}

