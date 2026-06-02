# Kafka Implementation Guide for Spring Boot

This guide provides detailed step-by-step instructions to implement Apache Kafka in your Spring Boot application.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Phase 1: Environment Setup](#phase-1-environment-setup)
3. [Phase 2: Project Configuration](#phase-2-project-configuration)
4. [Phase 3: Kafka Producer Implementation](#phase-3-kafka-producer-implementation)
5. [Phase 4: Kafka Consumer Implementation](#phase-4-kafka-consumer-implementation)
6. [Phase 5: Application Integration](#phase-5-application-integration)
7. [Phase 6: Testing](#phase-6-testing)
8. [Phase 7: Monitoring & Troubleshooting](#phase-7-monitoring--troubleshooting)

---

## Prerequisites

### Required Software
- **Java**: Version 17 or higher
- **Maven**: Version 3.6 or higher
- **Docker**: Latest version
- **Docker Compose**: Included with Docker Desktop
- **Git**: For version control
- **IDE**: IntelliJ IDEA, Eclipse, or VS Code

### System Requirements
- At least 4GB RAM available
- At least 2GB disk space for Docker images
- Network connectivity for downloading dependencies

### Knowledge Requirements
- Basic understanding of Spring Boot
- Familiarity with REST APIs
- Basic knowledge of message queuing concepts
- Understanding of Docker and containers

---

## Phase 1: Environment Setup

### Step 1.1: Clone or Initialize the Repository

```bash
# If cloning an existing repository
git clone https://github.com/rohithdeveloper/kafka-springboot.git
cd kafka-springboot

# Or initialize a new Spring Boot project
mkdir kafka-springboot
cd kafka-springboot
```

### Step 1.2: Verify Docker Installation

```bash
# Check Docker version
docker --version

# Check Docker Compose version
docker-compose --version

# Test Docker functionality
docker run hello-world
```

### Step 1.3: Create the docker-compose.yml File

Create a `docker-compose.yml` file in the project root:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    networks:
      - kafka-network
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    networks:
      - kafka-network
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 5

networks:
  kafka-network:
    driver: bridge
```

### Step 1.4: Start Kafka and Zookeeper

```bash
# Start services in detached mode
docker-compose up -d

# Verify services are running
docker-compose ps

# Check logs (if needed)
docker-compose logs -f

# Wait for services to be healthy (about 30 seconds)
```

---

## Phase 2: Project Configuration

### Step 2.1: Create Maven Project Structure

```bash
# Initialize Maven project structure
mkdir -p src/main/java/com/example/kafka/{config,model,producer,consumer,controller}
mkdir -p src/main/resources
mkdir -p src/test/java/com/example/kafka
```

### Step 2.2: Create pom.xml

Create `pom.xml` in the project root:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>kafka-springboot</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <name>Kafka Spring Boot Application</name>
    <description>Simple Spring Boot Application with Kafka Integration</description>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.1.5</version>
        <relativePath/>
    </parent>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <!-- Spring Boot Starters -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <!-- Lombok for reducing boilerplate -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <!-- JSON Processing -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-logging</artifactId>
        </dependency>

        <!-- Testing -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

### Step 2.3: Create application.yml Configuration

Create `src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: kafka-springboot-app
  
  kafka:
    bootstrap-servers: localhost:9092
    
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        linger.ms: 10
        batch.size: 16384
    
    consumer:
      bootstrap-servers: localhost:9092
      group-id: spring-boot-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        auto.offset.reset: earliest
        enable.auto.commit: true
        max.poll.records: 100
    
    admin:
      properties:
        bootstrap.servers: localhost:9092

server:
  port: 8080
  servlet:
    context-path: /

logging:
  level:
    root: INFO
    com.example.kafka: DEBUG
    org.springframework.kafka: INFO
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} - %msg%n"
```

### Step 2.4: Build and Verify Configuration

```bash
# Update Maven dependencies
mvn clean install

# Check for compilation errors
mvn compile
```

---

## Phase 3: Kafka Producer Implementation

### Step 3.1: Create Message Model

Create `src/main/java/com/example/kafka/model/Message.java`:

```java
package com.example.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Message {
    private String id;
    private String content;
    private LocalDateTime timestamp;
    private String sender;
    private String status; // PENDING, SENT, FAILED
}
```

### Step 3.2: Create Producer Service

Create `src/main/java/com/example/kafka/producer/MessageProducer.java`:

```java
package com.example.kafka.producer;

import com.example.kafka.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message as SpringMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {
    
    private static final String TOPIC = "messages";
    private final KafkaTemplate<String, Message> kafkaTemplate;
    
    /**
     * Send a simple message to Kafka
     */
    public void sendMessage(String content) {
        try {
            Message message = Message.builder()
                    .id(UUID.randomUUID().toString())
                    .content(content)
                    .timestamp(LocalDateTime.now())
                    .status("PENDING")
                    .build();
            
            kafkaTemplate.send(TOPIC, message.getId(), message)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Message sent successfully: {}", message);
                            message.setStatus("SENT");
                        } else {
                            log.error("Failed to send message: {}", message, ex);
                            message.setStatus("FAILED");
                        }
                    });
        } catch (Exception e) {
            log.error("Error sending message", e);
        }
    }
    
    /**
     * Send a message with custom sender
     */
    public void sendMessageWithSender(String content, String sender) {
        try {
            Message message = Message.builder()
                    .id(UUID.randomUUID().toString())
                    .content(content)
                    .sender(sender)
                    .timestamp(LocalDateTime.now())
                    .status("PENDING")
                    .build();
            
            kafkaTemplate.send(TOPIC, message.getId(), message)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Message sent by {}: {}", sender, message);
                            message.setStatus("SENT");
                        } else {
                            log.error("Failed to send message from {}: {}", sender, message, ex);
                            message.setStatus("FAILED");
                        }
                    });
        } catch (Exception e) {
            log.error("Error sending message", e);
        }
    }
    
    /**
     * Send message with custom headers
     */
    public void sendMessageWithHeaders(String content, String priority) {
        try {
            Message message = Message.builder()
                    .id(UUID.randomUUID().toString())
                    .content(content)
                    .timestamp(LocalDateTime.now())
                    .status("PENDING")
                    .build();
            
            SpringMessage<Message> springMessage = MessageBuilder
                    .withPayload(message)
                    .setHeader(KafkaHeaders.TOPIC, TOPIC)
                    .setHeader("priority", priority)
                    .setHeader("timestamp", System.currentTimeMillis())
                    .build();
            
            kafkaTemplate.send(springMessage)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Priority message sent [{}]: {}", priority, message);
                        } else {
                            log.error("Failed to send priority message: {}", message, ex);
                        }
                    });
        } catch (Exception e) {
            log.error("Error sending message with headers", e);
        }
    }
}
```

---

## Phase 4: Kafka Consumer Implementation

### Step 4.1: Create Consumer Service

Create `src/main/java/com/example/kafka/consumer/MessageConsumer.java`:

```java
package com.example.kafka.consumer;

import com.example.kafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.rebalance.ConsumerAware;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.apache.kafka.common.header.Headers;

@Slf4j
@Service
public class MessageConsumer {
    
    /**
     * Basic message listener
     */
    @KafkaListener(topics = "messages", groupId = "spring-boot-group")
    public void consumeMessage(@Payload Message message) {
        try {
            log.info("Message received from topic 'messages': {}", message);
            // Process the message
            processMessage(message);
        } catch (Exception e) {
            log.error("Error consuming message", e);
        }
    }
    
    /**
     * Consumer with partition assignment
     */
    @KafkaListener(topics = "messages", groupId = "spring-boot-group", 
                   concurrency = "3")
    public void consumeMessageWithPartition(
            @Payload Message message,
            @Headers org.springframework.messaging.handler.annotation.Headers headers) {
        try {
            log.info("Message received: ID={}, Content={}, Partition={}", 
                     message.getId(), 
                     message.getContent(), 
                     headers.get("kafka_receivedPartitionId"));
            processMessage(message);
        } catch (Exception e) {
            log.error("Error consuming message with partition info", e);
        }
    }
    
    /**
     * Process the received message
     */
    private void processMessage(Message message) {
        log.debug("Processing message with ID: {}", message.getId());
        // Add your business logic here
        // Examples:
        // - Save to database
        // - Validate message
        // - Trigger downstream operations
        // - Update metrics
        log.info("Message processed successfully");
    }
}
```

---

## Phase 5: Application Integration

### Step 5.1: Create Kafka Configuration Class

Create `src/main/java/com/example/kafka/config/KafkaConfig.java`:

```java
package com.example.kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import com.example.kafka.model.Message;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    /**
     * Create Kafka Admin topic
     */
    @Bean
    public NewTopic messagesTopic() {
        return TopicBuilder.name("messages")
                .partitions(3)
                .replicas(1)
                .config("retention.ms", "86400000") // 24 hours
                .config("segment.ms", "3600000")    // 1 hour
                .build();
    }
    
    /**
     * Producer factory configuration
     */
    @Bean
    public ProducerFactory<String, Message> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }
    
    /**
     * Kafka Template bean
     */
    @Bean
    public KafkaTemplate<String, Message> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```

### Step 5.2: Create REST Controller

Create `src/main/java/com/example/kafka/controller/MessageController.java`:

```java
package com.example.kafka.controller;

import com.example.kafka.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {
    
    private final MessageProducer messageProducer;
    
    /**
     * Send a simple message to Kafka
     */
    @PostMapping("/send")
    public ResponseEntity<Map<String, String>> sendMessage(
            @RequestParam String content) {
        try {
            messageProducer.sendMessage(content);
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Message sent successfully!");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error sending message", e);
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to send message: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    /**
     * Send message with sender information
     */
    @PostMapping("/send-with-sender")
    public ResponseEntity<Map<String, String>> sendMessageWithSender(
            @RequestParam String content,
            @RequestParam String sender) {
        try {
            messageProducer.sendMessageWithSender(content, sender);
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Message sent successfully from " + sender);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error sending message", e);
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to send message: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    /**
     * Send message with priority
     */
    @PostMapping("/send-with-priority")
    public ResponseEntity<Map<String, String>> sendMessageWithPriority(
            @RequestParam String content,
            @RequestParam(defaultValue = "normal") String priority) {
        try {
            messageProducer.sendMessageWithHeaders(content, priority);
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Priority message sent successfully!");
            response.put("priority", priority);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error sending message", e);
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to send message: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "UP");
        response.put("application", "kafka-springboot");
        return ResponseEntity.ok(response);
    }
}
```

### Step 5.3: Create Main Application Class

Create `src/main/java/com/example/kafka/KafkaSpringBootApplication.java`:

```java
package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KafkaSpringBootApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringBootApplication.class, args);
    }
}
```

---

## Phase 6: Testing

### Step 6.1: Build the Application

```bash
# Clean and build
mvn clean package

# Skip tests during initial build
mvn clean package -DskipTests
```

### Step 6.2: Run the Application

```bash
# Start the Spring Boot application
mvn spring-boot:run

# Or run the JAR file
java -jar target/kafka-springboot-1.0.0.jar
```

### Step 6.3: Test with cURL

```bash
# Test 1: Send a simple message
curl -X POST "http://localhost:8080/api/messages/send?content=Hello%20Kafka"

# Test 2: Send message with sender
curl -X POST "http://localhost:8080/api/messages/send-with-sender?content=Test%20Message&sender=TestUser"

# Test 3: Send message with priority
curl -X POST "http://localhost:8080/api/messages/send-with-priority?content=Urgent%20Message&priority=high"

# Test 4: Health check
curl -X GET "http://localhost:8080/api/messages/health"
```

### Step 6.4: Verify Logs

Check the application logs to confirm messages are being sent and consumed:

```
Expected log output:
- "Message sent successfully: Message(...)"
- "Message received from topic 'messages': Message(...)"
- "Message processed successfully"
```

### Step 6.5: Create Unit Tests

Create `src/test/java/com/example/kafka/producer/MessageProducerTest.java`:

```java
package com.example.kafka.producer;

import com.example.kafka.model.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = {"log.dir=/tmp/kafka-logs"})
public class MessageProducerTest {
    
    @Autowired
    private MessageProducer messageProducer;
    
    @Test
    public void testSendMessage() {
        assertDoesNotThrow(() -> {
            messageProducer.sendMessage("Test message");
        });
    }
}
```

---

## Phase 7: Monitoring & Troubleshooting

### Common Issues and Solutions

#### Issue 1: Connection Refused
```bash
# Solution: Verify Kafka is running
docker-compose ps

# Restart if needed
docker-compose restart kafka
```

#### Issue 2: Messages Not Being Consumed
```bash
# Check consumer group status
docker exec -it <kafka-container> kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Describe consumer group
docker exec -it <kafka-container> kafka-consumer-groups --bootstrap-server localhost:9092 --group spring-boot-group --describe
```

#### Issue 3: High Memory Usage
```bash
# Adjust JVM settings
export JAVA_OPTS="-Xmx256m -Xms256m"
mvn spring-boot:run
```

### Monitoring Commands

```bash
# List all topics
docker exec -it <kafka-container> kafka-topics --bootstrap-server localhost:9092 --list

# Describe a topic
docker exec -it <kafka-container> kafka-topics --bootstrap-server localhost:9092 --describe --topic messages

# Check broker metrics
docker logs <kafka-container> | grep -i "metric"

# View consumer lag
docker exec -it <kafka-container> kafka-consumer-groups --bootstrap-server localhost:9092 --group spring-boot-group --describe
```

### Performance Tuning Parameters

```yaml
# In application.yml, adjust these for optimization:
spring:
  kafka:
    producer:
      batch-size: 32768          # Increase for high throughput
      linger-ms: 100             # Wait longer for batch
      compression-type: snappy   # Enable compression
      acks: 1                     # Reduce latency (less durability)
    
    consumer:
      max-poll-records: 500       # Increase processing speed
      fetch-min-bytes: 32768      # Wait for more data
```

---

## Summary Checklist

- [ ] Docker and Docker Compose installed
- [ ] `docker-compose.yml` created and services running
- [ ] Maven project structure created
- [ ] `pom.xml` with all dependencies added
- [ ] `application.yml` configured
- [ ] Message model class created
- [ ] Producer service implemented
- [ ] Consumer service implemented
- [ ] Kafka configuration class created
- [ ] REST controller implemented
- [ ] Main application class created
- [ ] Application builds successfully
- [ ] Application starts without errors
- [ ] Messages can be sent via REST API
- [ ] Messages are consumed and logged
- [ ] Health check endpoint responds
- [ ] All tests pass

---

## Next Steps for Enhancement

1. **Add Database Integration**: Persist messages to a database
2. **Implement Error Handling**: Dead letter queues (DLQ)
3. **Add Metrics**: Integrate Micrometer for monitoring
4. **Security**: Implement authentication and encryption
5. **Distributed Tracing**: Add Spring Cloud Sleuth
6. **Event Sourcing**: Implement event sourcing pattern
7. **Performance Optimization**: Configure partition strategies
8. **Load Testing**: Test with high message volumes

---

## Resources and References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [Confluent Kafka Docker Images](https://hub.docker.com/r/confluentinc/cp-kafka)
- [Kafka Best Practices](https://kafka.apache.org/documentation/#bestpractices)

---

## Support and Community

- GitHub Issues: [kafka-springboot/issues](https://github.com/rohithdeveloper/kafka-springboot/issues)
- Stack Overflow: Tag with `kafka` and `spring-boot`
- Apache Kafka Community: [kafka.apache.org/community](https://kafka.apache.org/community)

