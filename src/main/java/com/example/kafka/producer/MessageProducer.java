package com.example.kafka.producer;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaTemplate<String, Message> kafkaTemplate;

    public void sendMessage(String content) {

        Message message = Message.builder()
                .id(UUID.randomUUID().toString())
                .content(content)
                .timestamp(System.currentTimeMillis())
                .build();

        org.springframework.messaging.Message<Message> kafkaMessage =
                MessageBuilder.withPayload(message)
                        .setHeader(KafkaHeaders.TOPIC, KafkaConfig.TOPIC_NAME)
                        .build();

        kafkaTemplate.send(kafkaMessage);

        log.info("Message sent: {}", message);
    }
}