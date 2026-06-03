package com.example.kafka.controller;

import com.example.kafka.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer messageProducer;

//    @PostMapping("/send")
//    public ResponseEntity<String> sendMessage(@RequestParam String content) {
//        messageProducer.sendMessage(content);
//        return ResponseEntity.ok("Message sent successfully!");
//    }
//
//    @PostMapping("/health")
//    public ResponseEntity<String> health() {
//        return ResponseEntity.ok("Application is running!");
//    }

        @GetMapping("/health")
        public ResponseEntity<String> health() {
            return ResponseEntity.ok("Application is running!");
        }

        @PostMapping("/send")
        public ResponseEntity<String> sendMessage(@RequestParam String content) {
            messageProducer.sendMessage(content);
            return ResponseEntity.ok("Message sent successfully!");
        }

        @PostMapping("/send/{message}")
        public ResponseEntity<String> sendMessageByPath(
                @PathVariable String message) {

            messageProducer.sendMessage(message);

            return ResponseEntity.ok("Message sent: " + message);
        }

        @PostMapping("/send-with-time")
        public ResponseEntity<String> sendMessageWithTime(
                @RequestParam String content) {

            String message =
                    content + " | Sent At : " + LocalDateTime.now();

            messageProducer.sendMessage(message);

            return ResponseEntity.ok("Message sent with timestamp");
        }

    @PostMapping("/bulk")
    public ResponseEntity<String> sendBulkMessages(
            @RequestParam(defaultValue = "10") int count) {

        IntStream.rangeClosed(1, count)
                .forEach(i -> messageProducer.sendMessage("Message-" + i));

        return ResponseEntity.ok(count + " messages sent successfully");
    }

        @GetMapping("/info")
        public ResponseEntity<String> info() {
            return ResponseEntity.ok("""
                Kafka Spring Boot Demo Application
                
                Available Endpoints:
                1. GET  /api/messages/health
                2. POST /api/messages/send?content=Hello
                3. POST /api/messages/send/{message}
                4. POST /api/messages/send-with-time?content=Hello
                5. POST /api/messages/bulk?count=10
                6. GET  /api/messages/info
                """);
        }
    }
