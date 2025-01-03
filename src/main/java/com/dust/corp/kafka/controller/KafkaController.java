package com.dust.corp.kafka.controller;

import com.dust.corp.kafka.sample.KafkaSyncService;
import com.dust.corp.kafka.service.dynamic.DynamicKafkaConsumer;
import com.dust.corp.kafka.service.dynamic.DynamicKafkaProducer;
import com.dust.corp.kafka.service.dynamic.KafkaTopicService;
import com.dust.corp.kafka.service.standard.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private KafkaTopicService topicService;

    @Autowired
    private DynamicKafkaConsumer dynamicConsumer;

    @Autowired
    private DynamicKafkaProducer dynamicProducer;

    @Autowired
    private KafkaSyncService kafkaSyncService;

    @GetMapping("/send")
    public String sendMessage(@RequestParam("message") String message) {
        kafkaProducer.sendMessage(message);
        return "Message sent to Kafka topic";
    }

    @PostMapping("/send")
    public ResponseEntity<String> sendMessage(
            @RequestParam String topicName,
            @RequestParam String message) throws InterruptedException {
        dynamicProducer.sendMessage(topicName, message);
        return ResponseEntity.ok("Message sent to topic: " + topicName);
    }

    @PostMapping("/create-and-subscribe")
    public ResponseEntity<String> createAndSubscribe(@RequestParam String topicName) {
        if (!topicService.doesTopicExist(topicName)) {
            topicService.createTopic(topicName, 1, (short) 1);
        } else {
            System.out.println("Topic: " + topicName + " exist!");
        }
        dynamicConsumer.subscribeToTopic(topicName);
        return ResponseEntity.ok("Topic created and subscribed: " + topicName);
    }

    @GetMapping("/start")
    public ResponseEntity<?> startProcess() throws InterruptedException {
        kafkaSyncService.startProcess();
        return ResponseEntity.ok().build();
    }

}
