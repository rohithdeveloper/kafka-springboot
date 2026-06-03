# Kafka Implementation Flow & Docker Architecture Guide

## Table of Contents
1. [System Architecture Overview](#system-architecture-overview)
2. [Docker Components Explained](#docker-components-explained)
3. [Kafka Message Flow](#kafka-message-flow)
4. [Spring Boot Application Flow](#spring-boot-application-flow)
5. [End-to-End Flow Diagram](#end-to-end-flow-diagram)
6. [Docker Networking](#docker-networking)
7. [Data Flow in Detail](#data-flow-in-detail)
8. [Troubleshooting & Monitoring](#troubleshooting--monitoring)

---

## System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        HOST MACHINE                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              Docker Container Environment                │  │
│  │                                                           │  │
│  │  ┌──────────────────┐    ┌──────────────────────────┐   │  │
│  │  │   Zookeeper      │    │    Kafka Broker          │   │  │
│  │  │   Container      │◄──►│    Container             │   │  │
│  │  │   Port: 2181     │    │    Port: 9092            │   │  │
│  │  └──────────────────┘    └──────────────────────────┘   │  │
│  │         ▲                          ▲                      │  │
│  │         │                          │                      │  │
│  │         └──────────────────────────┘                      │  │
│  │              (Docker Network: kafka-network)              │  │
│  └───────────────────────────────────────────────────────────┘  │
│                           ▲                                       │
│                           │ (Port Mapping)                        │
│  ┌────────────────────────┴────────────────────────────────┐    │
│  │                                                         │    │
│  │     Spring Boot Application (localhost:8080)           │    │
│  │     - MessageController                                │    │
│  │     - MessageProducer                                  │    │
│  │     - MessageConsumer                                  │    │
│  │     - KafkaTemplate                                    │    │
│  │                                                         │    │
│  └────────────────────────┬────────────────────────────────┘    │
│                           │                                       │
└───────────────────────────┼───────────────────────────────────────┘
                            │
                   HTTP Requests (curl/Postman)
```

---

## Docker Components Explained

### What is Docker?

Docker is a **containerization platform** that packages applications and their dependencies into isolated containers. For this Kafka project, Docker runs:

1. **Zookeeper Container** - Manages Kafka cluster
2. **Kafka Container** - Message broker

### Why Use Docker?

| Benefit | Explanation |
|---------|-------------|
| **Isolation** | Each service runs independently without affecting the host |
| **Reproducibility** | Same environment everywhere (dev, test, production) |
| **Ease of Setup** | No manual installation; everything pre-configured |
| **Port Mapping** | Container ports forwarded to host machine |
| **Easy Cleanup** | Delete container with one command, no residual files |

---

### Docker Compose Configuration Breakdown

#### File: `docker-compose.yml`

```yaml
version: '3.8'  # Docker Compose version

services:
  # ============================================
  # ZOOKEEPER SERVICE
  # ============================================
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    # Uses Confluent's pre-built Zookeeper image
    # This image has Zookeeper pre-installed and configured
    
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      # Port where Kafka broker connects to Zookeeper (inside container)
      # This is INTERNAL communication, not exposed to host
      
      ZOOKEEPER_TICK_TIME: 2000
      # Heartbeat interval in milliseconds
      # How often Zookeeper checks if services are alive
    
    ports:
      - "2181:2181"
      # Mapping: HOST_PORT:CONTAINER_PORT
      # Left (2181): Port on your computer
      # Right (2181): Port inside the container
      # Now you can access Zookeeper from localhost:2181
    
    networks:
      - kafka-network
      # Connects to custom Docker network
      # Allows containers to communicate by service name
    
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      # Command to check if Zookeeper is healthy
      # "nc -z localhost 2181" = check if port 2181 is open
      
      interval: 10s
      # Check health every 10 seconds
      
      timeout: 5s
      # Wait 5 seconds for response
      
      retries: 5
      # Try 5 times before marking as unhealthy

  # ============================================
  # KAFKA BROKER SERVICE
  # ============================================
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    # Confluent's pre-built Kafka broker image
    
    depends_on:
      zookeeper:
        condition: service_healthy
      # Wait for Zookeeper to be healthy before starting Kafka
      # This ensures proper initialization order
    
    ports:
      - "9092:9092"
      # HOST:CONTAINER mapping
      # Access Kafka from localhost:9092
    
    environment:
      # ═══════ Broker Configuration ═══════
      KAFKA_BROKER_ID: 1
      # Unique ID for this Kafka broker
      # If you have multiple brokers, each needs a different ID
      
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      # Connection string to Zookeeper
      # Uses service name "zookeeper" (Docker DNS resolution)
      # This is INTERNAL container communication
      
      # ═══════ Listener Configuration ═══════
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      # Tells clients where to connect to Kafka
      # PLAINTEXT://kafka:29092 = For containers within network (internal)
      # PLAINTEXT_HOST://localhost:9092 = For host machine (external)
      
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      # Maps listener names to security protocols
      # Both use PLAINTEXT (no encryption) for simplicity
      
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      # Internal broker-to-broker communication uses PLAINTEXT
      # Brokers talk to each other via this protocol
      
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      # How many replicas for internal topics
      # 1 = Only one copy (no replication)
      # OK for development, risky for production
      
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      # Automatically create topics when producer sends first message
      # Convenient for development, disabled in production for safety
    
    networks:
      - kafka-network
      # Same network as Zookeeper for communication
    
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"]
      # Command to verify Kafka is running and responsive
      # Attempts to get API versions from broker
      
      interval: 10s
      timeout: 10s
      retries: 5

# ============================================
# DOCKER NETWORK
# ============================================
networks:
  kafka-network:
    driver: bridge
    # Bridge network allows containers to communicate
    # Containers can reference each other by service name
```

---

### What Docker Does At Each Stage

#### **1. Initialization (docker-compose up -d)**

```
Step 1: Create Network
  └─> Docker creates a bridge network called "kafka-network"
      └─> Containers connected to this network can communicate
          using service names as hostnames

Step 2: Start Zookeeper Container
  └─> Docker pulls the image (if not cached)
  └─> Creates a container with:
      ├─ Zookeeper process running
      ├─ Port 2181 open inside container
      ├─ Environment variables set
      └─ Connected to kafka-network
      
  └─> Runs healthcheck every 10s:
      └─> Tries: nc -z localhost 2181
      └─> If succeeds: Container marked as "healthy"
      └─> If fails: Retries up to 5 times

Step 3: Start Kafka Container (after Zookeeper is healthy)
  └─> Docker pulls the image (if not cached)
  └─> Creates a container with:
      ├─ Kafka broker running
      ├─ Port 9092 open inside container
      ├─ Connection to Zookeeper established
      └─ Connected to kafka-network
      
  └─> Runs healthcheck every 10s:
      └─> Tries: kafka-broker-api-versions.sh --bootstrap-server localhost:9092
      └─> If succeeds: Container marked as "healthy"

Step 4: Port Mapping Established
  └─> Docker forwards:
      ├─ localhost:2181 ──> kafka-network:zookeeper:2181
      └─ localhost:9092 ──> kafka-network:kafka:9092
```

#### **2. Running State (docker-compose ps)**

```
When services are running, Docker maintains:

✅ Container Isolation: Each service in its own container
✅ Resource Limits: Memory/CPU allocated to each
✅ Port Forwarding: Host ports map to container ports
✅ Network Communication: Containers talk via kafka-network
✅ Persistent Monitoring: Healthchecks run periodically
```

#### **3. Shutdown (docker-compose down)**

```
Step 1: Signal Containers to Stop
  └─> Docker sends SIGTERM to each container
  └─> Services gracefully shut down

Step 2: Remove Containers
  └─> Containers are deleted
  └─> Data in containers is lost (unless persisted to volumes)

Step 3: Remove Network
  └─> kafka-network is deleted
  └─> All inter-container communication routes are removed

Note: Images remain for next startup (faster)
```

---

## Kafka Message Flow

### Flow Diagram

```
┌────────────────────────────────────────────────────────────┐
│                  KAFKA MESSAGE LIFECYCLE                    │
└────────────────────────────────────────────────────────────┘

1️⃣  PRODUCER SENDS MESSAGE
    │
    ├─ Message created in MessageProducer.sendMessage()
    ├─ Serialized to JSON format
    └─ Sent to Kafka topic: "messages"

2️⃣  KAFKA BROKER RECEIVES MESSAGE
    │
    ├─ Kafka assigns partition (0, 1, or 2)
    │  └─ Default: Hash(message_key) % num_partitions
    │     Key = Message UUID → determines partition
    │
    ├─ Message stored in partition log file
    │  └─ Location: /var/lib/kafka/data/messages-X/
    │
    ├─ Replication: Stored only on broker (1 replica)
    │
    ├─ Offset assigned: Sequential number in partition
    │  Example: messages-0: [offset 0, 1, 2, 3...]
    │
    └─ Acknowledgment sent to producer
       ✅ "Message persisted"

3️⃣  CONSUMER GROUP SUBSCRIBES
    │
    ├─ Group: "spring-boot-group"
    ├─ Subscribes to: "messages" topic
    ├─ Members: 1 consumer (can be scaled to 3 for each partition)
    │
    └─ Kafka assigns partitions:
       ├─ Consumer 1 → Partition 0
       ├─ Consumer 2 → Partition 1
       └─ Consumer 3 → Partition 2

4️⃣  CONSUMER FETCHES MESSAGES
    │
    ├─ Consumer polls from assigned partition
    ├─ Fetches messages starting from:
    │  └─ auto.offset.reset: earliest (start from beginning)
    │     or: latest (start from new messages only)
    │
    ├─ Message deserialized from JSON
    └─ Deserialization success → Process message

5️⃣  CONSUMER PROCESSES MESSAGE
    │
    ├─ MessageConsumer.consumeMessage() called
    ├─ @KafkaListener method invoked
    ├─ Message logged with details:
    │  ├─ Message ID
    │  ├─ Content
    │  └─ Timestamp
    │
    ├─ Business logic executed (in processMessage())
    └─ Processing complete

6️⃣  OFFSET COMMITTED
    │
    ├─ Consumer tracks current offset
    ├─ Configuration: enable.auto.commit: true
    ├─ Auto-commit interval: 5 seconds (default)
    │
    └─ Offset stored in Kafka __consumer_offsets topic
       ├─ Group: "spring-boot-group"
       ├─ Topic: "messages"
       ├─ Partition: 0, 1, or 2
       └─ Offset: Last successfully processed message position

7️⃣  REBALANCING (when consumer joins/leaves)
    │
    ├─ Zookeeper detects group membership change
    ├─ Triggers rebalancing
    ├─ New partition assignment
    └─ Consumers resume from committed offsets
```

---

## Spring Boot Application Flow

### Complete Request-Response Cycle

```
┌─────────────────────────────────────────────────────────────┐
│              REST API REQUEST ARRIVES                        │
└─────────────────────────────────────────────────────────────┘

POST /api/messages/send?content=Hello%20Kafka

                    ↓

┌─────────────────────────────────────────────────────────────┐
│  1. HTTP Request Handler (MessageController)                │
│  ─────────────────────��───────────────────────────────────  │
│  Method: sendMessage(String content)                        │
│                                                              │
│  ✅ Extract parameter: content = "Hello Kafka"              │
│  ✅ Validate input (basic check)                            │
│  ✅ Call messageProducer.sendMessage(content)               │
└─────────────────────────────────────────────────────────────┘

                    ↓

┌─────────────────────────────────────────────────────────────┐
│  2. Message Producer (MessageProducer.sendMessage)          │
│  ─────────────────────────────────────────────────────────  │
│  ✅ Create Message object:                                   │
│     ├─ ID: UUID.randomUUID()                                │
│     ├─ Content: "Hello Kafka"                               │
│     ├─ Timestamp: LocalDateTime.now()                       │
│     └─ Status: "PENDING"                                    │
│                                                              │
│  ✅ Serialize to JSON:                                       │
│     {                                                        │
│       "id": "550e8400-e29b-41d4-a716-446655440000",         │
│       "content": "Hello Kafka",                             │
│       "timestamp": "2026-06-03T06:15:00",                   │
│       "status": "PENDING"                                   │
│     }                                                        │
│                                                              │
│  ✅ Send to Kafka via KafkaTemplate                         │
│     ├─ Topic: "messages"                                    │
│     ├─ Key: Message.id (UUID)                               │
│     └─ Value: Serialized JSON                               │
│                                                              │
│  ✅ Log: "Message sent successfully"                        │
│  ✅ Update Status: "SENT"                                   │
└─────────────────────────────────────────────────────────────┘

                    ↓

┌─────────────────────────────────────────────────────────────┐
│  3. Kafka Producer Client (Spring Kafka)                    │
│  ─────────────────────────────────────────────────────────  │
│  ✅ Batch messages (configured batch.size = 16KB)           │
│  ✅ Optionally compress (compression-type: snappy)          │
│  ✅ Retry failed sends (retries: 3)                         │
│  ✅ Wait for acknowledgments (acks: all)                    │
│     ├─ Leader received: ack 1                               │
│     ├─ All replicas received: ack -1 (all)                  │
│     └─ For this setup: 1 replica, so acks immediately      │
└─────────────────────────────────────────────────────────────┘

                    ↓ (Network: localhost:9092)

┌─────────────────────────────────────────────────────────────┐
│  4. Kafka Broker (Docker Container)                         │
│  ─────────────────────────────────────────────────────────  │
│  ✅ Receives serialized message                             │
│  ✅ Determines partition:                                    │
│     └─ partition = hash(key) % 3 = hash(UUID) % 3          │
│     └─ Result: 0, 1, or 2 (round-robin effect)             │
│                                                              │
│  ✅ Appends to partition log file:                          │
│     ├─ Partition 0: messages-0/00000000000000000000.log    │
│     ├─ Partition 1: messages-1/00000000000000000000.log    │
│     └─ Partition 2: messages-2/00000000000000000000.log    │
│                                                              │
│  ✅ Assigns offset:                                          │
│     └─ New message gets next sequential offset              │
���     └─ Example: offset = 45 in partition 1                  │
│                                                              │
│  ✅ Stores in memory and disk                               │
│  ✅ Sends ProducerRecord with offset back                   │
│     ├─ Topic: "messages"                                    │
│     ├─ Partition: 1                                         │
│     ├─ Offset: 45                                           │
│     └─ Timestamp: server timestamp                          │
└─────────────────────────────────────────────────────────────┘

                    ↓ (Network: localhost:9092)

┌─────────────────────────────────────────────────────────────┐
│  5. Producer Callback (Spring Kafka)                        │
│  ─────────────────────────────────────────────────────────  │
│  ✅ Receives success acknowledgment                         │
│  ✅ Callback executes: .whenComplete((result, ex) -> {...})│
│  ✅ Updates Message status: "SENT"                          │
│  ✅ Logs: "Message sent successfully: Message(...)"         │
│  ✅ No exception thrown                                      │
└─────────────────────────────────────────────────────────────┘

                    ↓

┌─────────────────────────────────────────────────────────────┐
│  6. HTTP Response Sent to Client                            │
│  ─────────────────────────────────────────────────────────  │
│  Status: 200 OK                                             │
│  Body:                                                       │
│  {                                                           │
│    "status": "success",                                     │
│    "message": "Message sent successfully!"                  │
│  }                                                           │
│                                                              │
│  ⏱️  Response Time: ~10-50ms                                 │
└─────────────────────────────────────────────────────────────┘

    ↓ (In Parallel - Consumer automatically subscribes)

┌─────────────────────────────────────────────────────────────┐
│  7. Kafka Consumer (Docker Container)                       │
│  ─────────────────────────────────────────────────────────  │
│  ✅ Polls for new messages (~100ms poll interval)           │
│  ✅ Finds new message in assigned partition                 │
│  ✅ Fetches message:                                         │
│     ├─ Topic: "messages"                                    │
│     ├─ Partition: 1 (as assigned in partition 1)            │
│     └─ Offset: 45                                           │
│                                                              │
│  ✅ Deserializes JSON to Message object                     │
│     {                                                        │
│       "id": "550e8400-e29b-41d4-a716-446655440000",         │
│       "content": "Hello Kafka",                             │
│       "timestamp": "2026-06-03T06:15:00",                   │
│       "status": "PENDING"                                   │
│     }                                                        │
└─────────────────────────────────────────────────────────────┘

                    ↓

┌─────────────────────────────────────────────────────────────┐
│  8. Consumer Handler (@KafkaListener)                       │
│  ─────────────────────────────────────────────────────────  │
│  ✅ consumeMessage(@Payload Message message) invoked        │
│  ✅ Calls: processMessage(message)                          │
│  ✅ Logs: "Message received from topic 'messages': ..."    │
│  ✅ Logs: "Message processed successfully"                 │
│  ✅ Method completes without exception                      │
└─────────────────────────────────────────────────────────────┘

                    ↓

┌─────────────────────────────────────────────────────────────┐
│  9. Offset Commit (Auto-commit enabled)                     │
│  ─────────────────────────────────────────────────────────  │
│  ✅ Consumer commits offset: 45                             │
│  ✅ Stored in __consumer_offsets topic                      │
│     ├─ Group: "spring-boot-group"                          │
│     ├─ Topic: "messages"                                    │
│     ├─ Partition: 1                                         │
│     └─ Committed Offset: 45                                 │
│                                                              │
│  ✅ Next poll will start from offset 46                     │
│  ✅ If consumer restarts: begins from offset 46             │
│  ✅ No message reprocessing (idempotency maintained)        │
└─────────────────────────────────────────────────────────────┘

⏱️  Total End-to-End Time: ~50-200ms
   - Production: ~10-50ms
   - Consumer processing: ~30-100ms (+ business logic)
```

---

## End-to-End Flow Diagram

```
╔════════════════════════════════════════════════════════════════════════╗
║                      COMPLETE MESSAGE FLOW                             ║
╚════════════════════════════════════════════════════════════════════════╝

┌──────────────────────────────────────────────────────────────────────┐
│ CLIENT (Your Computer)                                                │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. User sends HTTP request via curl/Postman:                        │
│     ┌────────────────────────────────────────────────────────┐       │
│     │ POST /api/messages/send?content=Hello%20Kafka         │       │
│     │ Host: localhost:8080                                   │       │
│     └────────────────────────────────────────────────────────┘       │
│                          ↓                                             │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ SPRING BOOT APPLICATION (localhost:8080)                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  2. MessageController receives request                               │
│     ├─ Validates input                                               │
│     └─ Calls messageProducer.sendMessage("Hello Kafka")             │
│                                                                        │
│  3. MessageProducer.sendMessage()                                    │
│     ├─ Creates Message object (UUID, content, timestamp)            │
│     ├─ Serializes to JSON                                           │
│     ├─ Sends to Kafka via KafkaTemplate                             │
│     └─ Logs "Message sent successfully"                             │
│                          ↓                                             │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ DOCKER CONTAINER: KAFKA BROKER (9092)                                │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  4. Kafka Broker receives message                                    │
│     ├─ Message Key: UUID                                             │
│     ├─ Determines partition: hash(key) % 3 = 0/1/2                   │
│     ├─ Appends to partition log file                                 │
│     ├─ Assigns offset (incremental)                                  │
│     └─ Acknowledges to producer                                      │
│                          ↓                                             │
│  5. Message stored in partition:                                    │
│     ├─ Topic: messages                                               │
│     ├─ Partition: 0, 1, or 2                                         │
│     └─ Offset: e.g., 45                                              │
│                          ↓                                             │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ DOCKER CONTAINER: ZOOKEEPER (2181)                                   │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  6. Zookeeper coordinates (in background)                            │
│     ├─ Tracks broker status                                          │
│     ├─ Manages consumer group rebalancing                            │
│     ├─ Stores cluster metadata                                       │
│     └─ Monitoring: healthy/unhealthy                                 │
│                          ↓                                             │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ SPRING BOOT APPLICATION: Consumer                                     │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  7. MessageConsumer continuously polls Kafka                         │
│     ├─ Poll interval: ~100ms                                         │
│     ├─ Consumer group: "spring-boot-group"                           │
│     ├─ Assigned partition: 0/1/2 (auto-assigned)                    │
│     └─ Fetch from offset: last_committed_offset + 1                 │
│                          ↓                                             │
│  8. New message detected (offset 45)                                 │
│     ├─ Deserializes JSON to Message object                          │
│     ├─ Triggers @KafkaListener method                               │
│     └─ Calls: consumeMessage(message)                               │
│                          ↓                                             │
│  9. Message processing                                               │
│     ├─ Log: "Message received: ..."                                 │
│     ├─ Execute: processMessage()                                     │
│     └─ Log: "Message processed successfully"                        │
│                          ↓                                             │
│  10. Auto-commit offset (every 5 seconds by default)                 │
│      ├─ Commits offset 45 to Kafka                                   │
│      ├─ Stored in __consumer_offsets topic                          │
│      └─ Next poll starts from offset 46                             │
│                          ↓                                             │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ CLIENT (HTTP Response Back)                                            │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  11. HTTP 200 OK Response:                                           │
│      ┌────────────────────────────────────────────────────────┐      │
│      │ {                                                      │      │
│      │   "status": "success",                                │      │
│      │   "message": "Message sent successfully!"            │      │
│      │ }                                                      │      │
│      └────────────────────────────────────────────────────────┘      │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘

⏱️  TIMING BREAKDOWN:
    ├─ Request → Response: 10-50ms
    ├─ Message → Broker → Storage: 1-5ms
    └─ Broker → Consumer Detection: 100-200ms (poll interval dependent)
       └─ Total End-to-End: 150-300ms
```

---

## Docker Networking

### How Docker Containers Communicate

```
┌─────────────────────────────────────────────────────────────┐
│              DOCKER BRIDGE NETWORK: kafka-network           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌────────────────────┐         ┌─────────────────────┐   │
│  │   Zookeeper        │◄───────►│     Kafka Broker    │   │
│  │   Container        │         │     Container       │   │
│  │  (Service Name:    │         │  (Service Name:     │   │
│  │   "zookeeper")     │         │   "kafka")          │   │
│  │                    │         │                     │   │
│  │  Internal IP:      │         │  Internal IP:       │   │
│  │  172.XX.0.2        │         │  172.XX.0.3         │   │
│  └────────────────────┘         └─────────────────────┘   │
│           ▲                              ▲                 │
│           │                              │                 │
│           │                              │                 │
│      Port 2181                       Port 9092              │
│      (internal)                      (internal)             │
│           │                              │                 │
└───────────┼──────────────────────────────┼──────────────────┘
            │                              │
      ┌─────┴───────────────────────┬─────┴──────┐
      │                             │            │
      │ Port Mapping                │            │
      │ (Docker Host ↔ Container)   │            │
      │                             │            │
   localhost:2181 ◄─────────► 2181   │     localhost:9092 ◄─────► 9092
      │                             │            │
      │ Docker Engine               │            │
      │                             │            │
      └─────────────────────────────┴────────────┘
```

### Why Service Names Matter

**Inside Docker Network:**
- Kafka connects to Zookeeper using: `zookeeper:2181`
- Docker DNS resolves `zookeeper` → Zookeeper container's IP
- This is automatic and simpler than using IP addresses

**From Host Machine:**
- Spring Boot connects to Kafka using: `localhost:9092`
- Cannot use service name from outside network
- Must use `localhost` (or container IP)

### Port Mapping Explained

```yaml
# In docker-compose.yml:
services:
  kafka:
    ports:
      - "9092:9092"
      #  ↑    ↑
      #  │    └─ Container port (inside the container)
      #  └────── Host port (on your computer)

# If ports were "9093:9092":
# - Outside access: localhost:9093
# - Container still uses: 9092
# - Useful for running multiple Kafka instances!
```

---

## Data Flow in Detail

### 1. Message Serialization Flow

```
Java Object
    ↓
┌──────────────────────────────────────┐
│ Message {                            │
│   id: String (UUID)                  │
│   content: String                    │
│   timestamp: LocalDateTime           │
│   sender: String (optional)          │
│   status: String (PENDING/SENT)      │
│ }                                    │
└──────────────────────────────────────┘
    ↓
Serializer: JsonSerializer (configured in pom.xml)
    ↓
JSON String
    ↓
┌──────────────────────────────────────┐
│ {                                    │
│   "id": "550e8400-...",             │
│   "content": "Hello Kafka",          │
│   "timestamp": "2026-06-03T06:15", │
│   "sender": null,                    │
│   "status": "PENDING"                │
│ }                                    │
└──────────────────────────────────────┘
    ↓
Byte Array (UTF-8 encoded)
    ↓
Transmitted over network (9092)
```

### 2. Message Partition Assignment

```
Message Key = UUID (e.g., "550e8400-e29b-41d4-a716-446655440000")

Hash Function:
  └─ hash(key) = some large integer
  └─ Example: 12857394521

Partition Assignment:
  └─ partition = hash(key) % number_of_partitions
  └─ partition = 12857394521 % 3
  └─ partition = 0 (or 1 or 2)

Result:
  └─ Message goes to partition 0
  └─ All messages with same key go to same partition
  └─ Ordering guaranteed within partition
  └─ Partitions filled round-robin for different keys
```

### 3. Offset Tracking

```
Consumer Group: spring-boot-group
Partition Assignment: 
  └─ If 1 consumer: gets all 3 partitions
  └─ If 3 consumers: each gets 1 partition

Offset Tracking per Partition:
  ├─ Partition 0: Committed offset = 42
  │  ├─ Next fetch: start from offset 43
  │  └─ Messages 0-42: already processed
  │
  ├─ Partition 1: Committed offset = 85
  │  ├─ Next fetch: start from offset 86
  │  └─ Messages 0-85: already processed
  │
  └─ Partition 2: Committed offset = 0
     ├─ Next fetch: start from offset 1
     └─ Messages 0: already processed

Offset Commit:
  └─ After successfully processing a message
  └─ Offset automatically committed (every 5 seconds)
  └─ Stored in internal Kafka topic: __consumer_offsets
  └─ Survives consumer restarts!
```

### 4. Consumer Lag Calculation

```
Consumer Lag = Log End Offset (LEO) - Current Offset

Example:
  ├─ Partition 1
  ├─ Messages sent: 0 to 100 (LEO = 101)
  ├─ Last processed: offset 85
  ├─ LAG = 101 - 86 = 15 messages

Interpretation:
  ├─ Lag = 0: Consumer caught up (no backlog)
  ├─ Lag > 0: Consumer behind (backlog exists)
  ├─ Lag = Large: Consumer very behind (maybe crashed)

Monitoring:
  └─ docker exec -it kafka kafka-consumer-groups \
       --bootstrap-server localhost:9092 \
       --group spring-boot-group \
       --describe
```

---

## Troubleshooting & Monitoring

### Common Issues and Docker Solutions

#### Issue 1: "Connection Refused: localhost:9092"

```
Problem:
  └─ Spring Boot cannot reach Kafka broker

Root Causes:
  1. Docker containers not running
  2. Kafka failed to start
  3. Port mapping incorrect
  4. Firewall blocking

Solution:
┌────────────────────────────────────────┐
│ # Check containers status             │
│ docker-compose ps                      │
│                                        │
│ # Check Kafka logs                     │
│ docker-compose logs kafka              │
│                                        │
│ # Restart Kafka                        │
│ docker-compose restart kafka           │
│                                        │
│ # Test connection from host            │
│ nc -zv localhost 9092                  │
└────────────────────────────────────────┘
```

#### Issue 2: "Messages Not Being Consumed"

```
Diagnosis:
  1. Check if producer sent messages:
     └─ Look for "Message sent successfully" in logs
  
  2. Check consumer group status:
     └─ docker exec -it kafka kafka-consumer-groups \
          --bootstrap-server localhost:9092 \
          --group spring-boot-group \
          --describe
  
  3. Check for errors in consumer logs:
     └─ docker-compose logs app | grep ERROR

Common Causes:
  ├─ Consumer not started
  ├─ Wrong topic name
  ├─ Consumer group lag increasing (producer faster than consumer)
  ├─ Deserialization error
  ├─ Exception in processMessage() method

Solutions:
┌────────────────────────────────────────┐
│ # Check topic messages                 │
│ docker exec -it kafka \                │
│   kafka-console-consumer \             │
│   --bootstrap-server localhost:9092 \  │
│   --topic messages \                   │
│   --from-beginning                     │
│                                        │
│ # View recent messages only            │
│ docker exec -it kafka \                │
│   kafka-console-consumer \             │
│   --bootstrap-server localhost:9092 \  │
│   --topic messages \                   │
│   --max-messages 10                    │
└────────────────────────────────────────┘
```

#### Issue 3: "High Memory Usage"

```
Docker Memory Monitoring:
┌────────────────────────────────────────┐
│ # View container resource usage        │
│ docker stats                           │
│                                        │
│ # View only Kafka and Zookeeper        │
│ docker stats kafka zookeeper           │
└────────────────────────────────────────┘

Typical Consumption:
  ├─ Zookeeper: 300-500 MB
  ├─ Kafka: 600-1000 MB
  ├─ Spring Boot: 200-400 MB

If exceeding limits:
  1. Check for memory leaks in application
  2. Reduce max.poll.records in consumer
  3. Increase Docker memory allocation
     └─ Docker Desktop Settings → Resources
```

### Monitoring Commands

```bash
# 1. Container Health Status
docker-compose ps

# 2. Service Logs
docker-compose logs -f kafka      # Follow Kafka logs
docker-compose logs -f zookeeper  # Follow Zookeeper logs
docker-compose logs kafka --tail 100  # Last 100 lines

# 3. Topic Management
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic messages

# 4. Consumer Group Monitoring
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group spring-boot-group --describe

# 5. Message Count
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic messages --max-messages 1 --timeout-ms 5000

# 6. Network Connectivity Test
docker exec -it kafka nc -zv localhost 2181  # Zookeeper
docker exec -it kafka nc -zv kafka 29092     # Internal Kafka
docker exec -it zookeeper nc -zv kafka 29092 # From Zookeeper to Kafka

# 7. Docker Network Inspection
docker network ls
docker network inspect kafka-network
```

---

## Summary: What Each Component Does

| Component | Purpose | Port (Container) | Port (Host) | Dependency |
|-----------|---------|------------------|-------------|------------|
| **Zookeeper** | Cluster coordination, metadata management | 2181 | 2181 | None |
| **Kafka** | Message broker, storage, distribution | 9092 (client) / 29092 (broker) | 9092 | Zookeeper |
| **Spring Boot App** | REST API, producer, consumer logic | 8080 | 8080 | Kafka |

---

## Complete Data Path: Step by Step

```
USER
  │
  ├─> curl -X POST "http://localhost:8080/api/messages/send?content=Hello%20Kafka"
  │
  ▼
[HOST MACHINE]
  │
  ├─> Port 8080 routes to Spring Boot Application
  │
  ▼
[SPRING BOOT - MessageController]
  │
  ├─> Receives HTTP request
  ├─> Calls messageProducer.sendMessage("Hello Kafka")
  │
  ▼
[SPRING BOOT - MessageProducer]
  │
  ├─> Creates Message object
  ├─> Serializes to JSON
  ├─> Sends via KafkaTemplate
  │
  ▼
[KAFKA CLIENT CONNECTION]
  │
  ├─> Connects to localhost:9092
  │
  ▼
[DOCKER PORT MAPPING]
  │
  ├─> localhost:9092 → Kafka Container:9092
  │
  ▼
[KAFKA BROKER - Docker Container]
  │
  ├─> Receives message
  ├─> Determines partition (hash-based)
  ├─> Stores in partition log
  ├─> Assigns offset
  ├─> Sends acknowledgment
  │
  ▼
[DOCKER BRIDGE NETWORK: kafka-network]
  │
  ├─> Zookeeper aware of message
  ├─> Coordinates with consumer group
  │
  ▼
[KAFKA CONSUMER - Polling]
  │
  ├─> Polls for new messages
  ├─> Detects message in partition
  ├─> Fetches message
  │
  ▼
[SPRING BOOT - MessageConsumer]
  │
  ├─> @KafkaListener triggered
  ├─> Deserializes JSON to Message
  ├─> Executes processMessage()
  ├─> Commits offset
  │
  ▼
[RESPONSE TO USER]
  │
  └─> HTTP 200 OK with success message
```

---

## Key Takeaways

### Docker's Role
- **Isolation**: Kafka and Zookeeper run independently
- **Networking**: Docker network enables container communication via service names
- **Port Mapping**: Exposes container ports to host machine
- **Health Checks**: Automatically monitors service health
- **Easy Cleanup**: Single command to tear down entire stack

### Message Flow
1. **Producer** creates message → sends to Kafka
2. **Kafka** receives → assigns partition → stores with offset
3. **Zookeeper** coordinates → manages metadata
4. **Consumer** polls → fetches message → processes
5. **Offset** commits → tracks progress

### Architecture Benefits
- **Scalability**: Can add consumers (up to partition count)
- **Durability**: Messages persist on disk
- **Ordering**: Guaranteed within partition
- **Reliability**: Acknowledgments ensure delivery
- **Monitoring**: Built-in consumer group tracking

---

## Useful Docker Commands Reference

```bash
# Start services
docker-compose up -d

# View running services
docker-compose ps

# View logs
docker-compose logs -f

# Stop services
docker-compose stop

# Stop and remove
docker-compose down

# Stop and remove including volumes
docker-compose down -v

# Rebuild services
docker-compose up -d --build

# Execute command in container
docker exec -it kafka bash
docker exec -it zookeeper bash

# View resource usage
docker stats
```

This comprehensive guide covers the complete architecture and data flow of your Kafka Spring Boot implementation with Docker!
