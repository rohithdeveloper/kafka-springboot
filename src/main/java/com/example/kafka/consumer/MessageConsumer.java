package com.example.kafka.consumer;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MessageConsumer {

    @KafkaListener(topics = KafkaConfig.TOPIC_NAME, groupId = "spring-boot-group")
    public void consume(Message message) {
        log.info("Message received: id={}, content={}, timestamp={}", 
                message.getId(), 
                message.getContent(), 
                message.getTimestamp());
    }
}