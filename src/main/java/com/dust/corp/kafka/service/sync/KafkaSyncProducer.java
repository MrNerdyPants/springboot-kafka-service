package com.dust.corp.kafka.service.sync;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaSyncProducer {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private KafkaProducer<String, String> producer;
    private final ConcurrentHashMap<String, CountDownLatch> latchMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> responseMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
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
        latch.await(); // OR
//        latch.await(10, TimeUnit.SECONDS);// Wait for the consumer to notify

        String response = responseMap.remove(correlationId); // Retrieve and remove the response
        latchMap.remove(correlationId); // Cleanup the latch
        return response;
    }

    public void sendMessage(String correlationId, String topic, String value) throws InterruptedException {

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, correlationId, value);

        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception == null) {
                System.out.println("Message sent successfully with Correlation ID: " + correlationId);
            } else {
                exception.printStackTrace();
            }
        });
    }

    public void close() {
        producer.close();
    }

}

