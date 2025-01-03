package com.dust.corp.kafka.sample.service;

import com.dust.corp.kafka.sample.Charge;
import com.dust.corp.kafka.sample.entity.User;
import com.dust.corp.kafka.sample.entity.repository.UserRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@Setter
@Service
public class KafkaSyncConsumer {
    private KafkaConsumer<String, String> consumer;
    private  ConcurrentHashMap<String, CountDownLatch> latchMap;
    private  ConcurrentHashMap<String, String> responseMap;

    private final ConcurrentHashMap<String, String> consumedMessages;

    private ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionService transactionService;

    public KafkaSyncConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "notificationGroup");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList("notification_topic2"));
        this.latchMap = null;
        this.responseMap = null;
        consumedMessages = new ConcurrentHashMap<>();
    }

    public void listenForResponses() {
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String correlationId = record.key(); // Extract the correlation ID
                    String response = record.value();

                    consumedMessages.put(correlationId, response);

                    System.out.println("Consumer received: " + response + " with Correlation ID: " + correlationId);

                }
                latchMap.keySet().forEach(key -> {
                    if (consumedMessages.containsKey(key)) {
                        Charge charge = null;
                        try {
                            charge = mapper.readValue(consumedMessages.get(key), Charge.class);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        User user = userRepository.findById(charge.getUserId()).orElseThrow(() -> new RuntimeException("User not Exist"));
                        transactionService.chargeTransaction(user, charge);
                        userRepository.save(user);
                        responseMap.put(key, consumedMessages.get(key)); // Save the response
                        latchMap.get(key).countDown(); // Notify the waiting producer

                        consumedMessages.remove(key);
                    }
                });
            }
        }).start();
    }

    public void close() {
        consumer.close();
    }
}

