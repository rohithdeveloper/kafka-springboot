# Simple Spring Boot Application with Kafka

This is a simple Spring Boot application that demonstrates Kafka integration with producer and consumer functionality.

## Prerequisites

- Java 17+
- Maven 3.6+
- Docker (for running Kafka)

## Project Structure

```
kafka-springboot/
├── src/main/java/com/example/kafka/
│   ├── KafkaSpringBootApplication.java     # Main application class
│   ├── config/
│   │   └── KafkaConfig.java                # Kafka configuration and topic setup
│   ├── model/
│   │   └── Message.java                    # Message data model
│   ├── producer/
│   │   └── MessageProducer.java            # Kafka producer service
│   ├── consumer/
│   │   └── MessageConsumer.java            # Kafka consumer service
│   └── controller/
│       └── MessageController.java          # REST controller
├── src/main/resources/
│   └── application.yml                     # Application configuration
├── docker-compose.yml                      # Docker compose for Kafka/Zookeeper
└── pom.xml                                 # Maven dependencies
```

## Quick Start

### 1. Start Kafka and Zookeeper

```bash
docker-compose up -d
```

This will start:
- Zookeeper on port 2181
- Kafka on port 9092

### 2. Build and Run the Application

```bash
mvn clean install
mvn spring-boot:run
```

The application will start on `http://localhost:8080`

### 3. Test the Application

#### Send a message:
```bash
curl -X POST "http://localhost:8080/api/messages/send?content=Hello%20Kafka"
```

#### Check application health:
```bash
curl -X POST "http://localhost:8080/api/messages/health"
```

### 4. View Logs

When you send a message, you should see logs like:
```
MessageProducer: Message sent: Message(id=xxx, content=Hello Kafka, timestamp=1234567890)
MessageConsumer: Message received: id=xxx, content=Hello Kafka, timestamp=1234567890
```

## Features

- **KafkaConfig**: Automatically creates a topic named "messages" with 3 partitions
- **MessageProducer**: Sends JSON-serialized messages to Kafka
- **MessageConsumer**: Listens to messages from Kafka and logs them
- **MessageController**: REST API to send messages
- **Docker Compose**: Easy setup of Kafka and Zookeeper

## Configuration

The application is configured in `application.yml`:
- Bootstrap servers: `localhost:9092`
- Consumer group: `spring-boot-group`
- Topic: `messages`
- Partitions: 3
- Serialization: JSON

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/messages/send` | POST | Send a message to Kafka |
| `/api/messages/health` | POST | Check application health |

### Send Message Request

```
POST /api/messages/send?content=Your%20message%20here
```

**Response:**
```json
{
  "message": "Message sent successfully!"
}
```

## Stopping Kafka

```bash
docker-compose down
```

## Key Components

### Message Model
A simple POJO with ID, content, and timestamp.

### Producer Pattern
Uses `KafkaTemplate` to send messages to the Kafka topic.

### Consumer Pattern
Uses `@KafkaListener` annotation to consume messages from the topic.

### Configuration
- Auto-creates topics on startup
- Configures JSON serialization/deserialization
- Sets up consumer groups

## Next Steps

- Add error handling and retry logic
- Implement custom partitioning strategies
- Add database persistence
- Implement event sourcing
- Add metrics and monitoring

## License

This project is open source and available under the MIT License.