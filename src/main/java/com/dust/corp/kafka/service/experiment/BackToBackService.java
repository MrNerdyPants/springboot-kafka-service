package com.dust.corp.kafka.service.experiment;

import com.dust.corp.kafka.service.dynamic.DynamicKafkaConsumer;
import com.dust.corp.kafka.service.dynamic.DynamicKafkaProducer;
import com.dust.corp.kafka.service.dynamic.KafkaTopicService;
import com.dust.corp.kafka.service.sync.KafkaSyncProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class BackToBackService {

    @Autowired
    private KafkaTopicService topicService;

    @Autowired
    private DynamicKafkaConsumer dynamicConsumer;

    @Autowired
    private DynamicKafkaProducer dynamicProducer;

    @Autowired
    private KafkaSyncProducer kafkaSyncProducer;

    public void createAndSubscribeAndSend(String topicToSubscribe) throws InterruptedException {
        dynamicConsumer.subscribeToTopic(topicToSubscribe);

        String topicName = UUID.randomUUID().toString();
        Boolean topicExist = topicService.doesTopicExist(topicName);
        if (!topicExist) {
            topicService.createTopic(topicName, 1, (short) 1);
        } else {
            System.out.println("Topic: " + topicName + " exist!");
        }
        dynamicConsumer.subscribeToTopic(topicName);

        dynamicProducer.sendMessage(topicToSubscribe, topicName);
    }

    public void backToBack(String topicToSubscribe) throws InterruptedException {
        dynamicConsumer.subscribeToTopic(topicToSubscribe);


        String topicName = getTopic(topicToSubscribe);//UUID.randomUUID().toString();
        Boolean topicExist = topicService.doesTopicExist(topicName);
        if (!topicExist) {
            topicService.createTopic(topicName, 1, (short) 1);
        } else {
            System.out.println("Topic: " + topicName + " exist!");
        }
        dynamicConsumer.subscribeToTopic(topicName);

        kafkaSyncProducer.sendMessage(topicToSubscribe, topicName);
    }

    private String getTopic(String currentTopic) {

        if (currentTopic.equalsIgnoreCase(TopicName.D_TOPIC_1.topic)) {
            return TopicName.D_TOPIC_2.topic;
        } else {
            return TopicName.D_TOPIC_1.topic;
        }
    }

    private enum TopicName {
        D_TOPIC_1("d-topic-1"),
        D_TOPIC_2("d-topic-2");

        TopicName(String topic) {
            this.topic = topic;
        }

        private String topic;
    }

}
