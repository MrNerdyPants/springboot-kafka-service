package com.dust.corp.kafka.service.standard;

import com.dust.corp.kafka.service.dynamic.DynamicKafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @Autowired
    private DynamicKafkaProducer kafkaProducer;

    @KafkaListener(topics = "test_topic", groupId = "group_id")
    public void consume(String message) throws InterruptedException {
        System.out.println("Consumed message: " + message);
        kafkaProducer.sendMessage("test_topic2", message);
    }

}
