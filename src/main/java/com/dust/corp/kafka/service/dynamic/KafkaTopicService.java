package com.dust.corp.kafka.service.dynamic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaTopicService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * This method is used to create new topics.
     *
     * @param topicName
     * @param partitions
     * @param replicationFactor
     */
    public void createTopic(String topicName, int partitions, short replicationFactor) {
        try (AdminClient adminClient = AdminClient.create(Collections.singletonMap("bootstrap.servers", bootstrapServers))) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.println("Topic created: " + topicName);
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Failed to create topic: " + e.getMessage());
        }
    }

    public boolean doesTopicExist(String topicName) {
        try (AdminClient adminClient = AdminClient.create(Collections.singletonMap("bootstrap.servers", bootstrapServers))) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(false); // Exclude internal topics
            ListTopicsResult topics = adminClient.listTopics(options);
            Set<String> topicNames = topics.names().get();
            return topicNames.contains(topicName);
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Failed to check topic existence: " + e.getMessage());
            return false;
        }
    }

    /**
     * This method is used to get if a topic is subscribed.
     * Avoid calling this method frequently, as creating a new consumer for each check may add overhead.
     *
     * @param topicName
     * @return
     */
    public boolean isTopicSubscribed(String topicName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Set<String> subscribedTopics = consumer.subscription();
            return subscribedTopics.contains(topicName);
        } catch (Exception e) {
            System.err.println("Error checking subscription for topic: " + e.getMessage());
            return false;
        }
    }
}

