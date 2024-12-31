package com.dust.corp.kafka;

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

    @GetMapping("/send")
    public String sendMessage(@RequestParam("message") String message) {
        kafkaProducer.sendMessage(message);
        return "Message sent to Kafka topic";
    }

    @PostMapping("/send")
    public ResponseEntity<String> sendMessage(
            @RequestParam String topicName,
            @RequestParam String message) {
        dynamicProducer.sendMessage(topicName, message);
        return ResponseEntity.ok("Message sent to topic: " + topicName);
    }

    @PostMapping("/create-and-subscribe")
    public ResponseEntity<String> createAndSubscribe(@RequestParam String topicName) {
        topicService.createTopic(topicName, 1, (short) 1);
        dynamicConsumer.subscribeToTopic(topicName);
        return ResponseEntity.ok("Topic created and subscribed: " + topicName);
    }

}
