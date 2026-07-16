# Saga Pattern POC: Distributed Transactions with MongoDB & Kafka KRaft

## 📋 Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Why This Pattern?](#why-this-pattern)
4. [ACID Properties in Distributed Systems](#acid-properties-in-distributed-systems)
5. [How It Works](#how-it-works)
6. [Technology Stack](#technology-stack)
7. [Setup & Installation](#setup--installation)
8. [Testing the Flow](#testing-the-flow)
9. [Understanding the Code](#understanding-the-code)
10. [Common Scenarios](#common-scenarios)
11. [Troubleshooting](#troubleshooting)

---

## Overview

This is a **Proof of Concept (POC)** demonstrating how to maintain **ACID (Atomicity, Consistency, Isolation, Durability)** properties across distributed microservices using the **Saga Pattern** with **MongoDB Transactions** and **Apache Kafka (KRaft mode)**.

### The Problem We're Solving

In a microservices architecture, each service has its own database. When a business transaction spans multiple services (e.g., creating a booking and processing payment), we face the **distributed transaction problem**:

```
❌ Traditional SQL approach:
- Each service has its own database
- SQL transactions are LOCAL (can't span across services)
- If Payment fails after Booking commits, how do we rollback?

✅ Our Solution:
- Use Saga Pattern with compensating transactions
- Event-driven communication via Kafka
- MongoDB transactions for local ACID guarantees
```

---

## Architecture

### System Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  ┌─────────────┐        ┌─────────────┐        ┌─────────────┐  │
│  │   Client    │        │   Kafka     │        │   Client    │  │
│  │   (POST)    │        │  (KRaft)    │        │   (GET)     │  │
│  └──────┬──────┘        └──────┬──────┘        └──────▲──────┘  │
│         │                      │                      │         │
│         ▼                      │                      │         │
│  ┌─────────────┐     ┌─────────▼────────┐     ┌─────────────┐   │
│  │  Booking    │     │  booking-events  │     │  Booking    │   │
│  │  Service    │────▶│  Topic           │────▶│  Service    │   │
│  │  (Port 3001)│     └──────────────────┘     │  (Read)     │   │
│  └──────┬──────┘                              └─────────────┘   │
│         │                                                       │
│         │  MongoDB Transaction                                  │
│         │  (Local ACID)                                         │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │  MongoDB    │                                                │
│  │  booking_db │                                                │
│  └─────────────┘                                                │
│                                                                 │
│         ┌────────────────────────────────┐                      │
│         │                                │                      │
│         ▼                                ▼                      │
│  ┌─────────────┐               ┌──────────────────┐             │
│  │  Payment    │               │  payment-events  │             │
│  │  Service    │◀─────────────▶│  Topic           │             │
│  │  (Port 3002)│               └──────────────────┘             │
│  └──────┬──────┘                                                │
│         │                                                       │
│         │  MongoDB Transaction                                  │
│         │  (Local ACID)                                         │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │  MongoDB    │                                                │
│  │  payment_db │                                                │
│  └─────────────┘                                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Event Flow: Success Path

```
1. Client POST /api/bookings
   └─> Booking Service creates booking (PENDING) in MongoDB transaction
   
2. Booking Service emits BOOKING_CREATED event
   └─> Kafka Topic: booking-events
   
3. Payment Service consumes BOOKING_CREATED
   └─> Processes payment in MongoDB transaction
   
4. Payment Service emits PAYMENT_SUCCESS event
   └─> Kafka Topic: payment-events
   
5. Booking Service consumes PAYMENT_SUCCESS
   └─> Updates booking status to CONFIRMED
```

### Event Flow: Failure Path (Compensation)

```
1. Client POST /api/bookings
   └─> Booking Service creates booking (PENDING) in MongoDB transaction
   
2. Booking Service emits BOOKING_CREATED event
   └─> Kafka Topic: booking-events
   
3. Payment Service consumes BOOKING_CREATED
   └─> ❌ Payment fails (simulated or real error)
   
4. Payment Service emits PAYMENT_FAILED event
   └─> Kafka Topic: payment-events
   
5. Booking Service consumes PAYMENT_FAILED
   └─> 💡 COMPENSATION: Updates booking status to CANCELLED
   └─> This is the "rollback" in distributed systems
```

---

## Why This Pattern?

### The Problem: Distributed Transactions

In a monolithic application, you'd simply do:

```sql
BEGIN TRANSACTION;
  INSERT INTO bookings ...;
  INSERT INTO payments ...;
  UPDATE inventory ...;
COMMIT;  -- All or nothing
```

But in microservices:
----------------------
```
❌ Can't do this:
Service A: BEGIN TRANSACTION; INSERT INTO bookings...; 
           CALL Service_B();  ❌ Can't call remote service in transaction
           COMMIT;

✅ Must do this:
Service A: Create booking (local transaction)
           Send event to Kafka
Service B: Process payment (local transaction)
           Send result event
Service A: Update booking based on result (compensation if needed)
```

### Why Saga Pattern?

| Feature | Traditional 2PC | Saga Pattern |
|---------|----------------|--------------|
| **Blocking** | Yes (locks resources) | No (non-blocking) |
| **Performance** | Slow (multiple round trips) | Fast (async events) |
| **Availability** | Low (coordinator single point) | High (decentralized) |
| **Complexity** | High | Moderate |
| **Rollback** | Immediate | Compensating actions |
| **Best For** | Short, critical transactions | Long-running business processes |

---

## ACID Properties in Distributed Systems

### How We Achieve Each Property

#### 1. **Atomicity** (All or Nothing)

**Local Level:** MongoDB transactions ensure atomicity within each service

```javascript
// In Booking Service
await session.withTransaction(async () => {
    // Either ALL of these succeed...
    await bookings.insertOne(booking, { session });
    await events.insertOne(event, { session });
    // ...or NONE persist (auto-rollback on error)
});

// In Payment Service
await session.withTransaction(async () => {
    await payments.insertOne(payment, { session });
    await events.insertOne(event, { session });
    // Both or nothing
});
```

**Global Level:** Saga pattern ensures eventual consistency

```javascript
// If Payment fails, compensation triggers
if (event.eventType === 'PAYMENT_FAILED') {
    // Compensation: Cancel the booking
    await bookings.updateOne(
        { _id: bookingId },
        { $set: { status: 'CANCELLED' } }
    );
}
```

#### 2. **Consistency** (Valid State)

**Business Rules Enforced:**

```javascript
// Booking states
PENDING → CONFIRMED  (Payment success)
PENDING → CANCELLED  (Payment failure)
PENDING → EVENT_FAILED (Kafka failure)

// Payment states
PENDING → COMPLETED  (Success)
PENDING → FAILED     (Error)
```

**Data Validation:**

```javascript
// MongoDB schema validation (implicit)
const booking = {
    userId: 'string',      // Required
    amount: 'number > 0',  // Must be positive
    status: 'enum',        // Only allowed values
    createdAt: 'date'      // Auto-generated
};
```

#### 3. **Isolation** (Transactions Don't Interfere)

**MongoDB Isolation Level:**

```javascript
// MongoDB uses snapshot isolation
// Each transaction sees a consistent snapshot
// No dirty reads or write conflicts

// Example: Two concurrent bookings
// Transaction 1: Creates booking #100
// Transaction 2: Creates booking #101
// Both are isolated - no interference
```

**Event Ordering:**

```javascript
// Kafka guarantees partition-level ordering
// Events for same booking ID go to same partition
// This ensures correct sequence:
// 1. BOOKING_CREATED
// 2. PAYMENT_SUCCESS or PAYMENT_FAILED
// 3. Final status update
```

#### 4. **Durability** (Persisted Data)

**MongoDB Durability:**

```javascript
// Data is written to disk
await session.commitTransaction();

// Even if service crashes, data persists
// MongoDB WAL (Write-Ahead Logging) ensures durability
```

**Kafka Durability:**

```javascript
// Messages are persisted to disk
await producer.send({
    topic: 'booking-events',
    messages: [{ value: eventData }]
});

// Even if consumers are offline, messages persist
// Configurable retention period
```

---

## How It Works

### Step-by-Step Transaction Flow

#### 1. **Booking Creation (Local Transaction)**

```javascript
// booking-service/index.js: POST /api/bookings

const session = mongoClient.startSession();

await session.withTransaction(async () => {
    // Step 1: Insert booking
    const booking = {
        userId: 'user-123',
        amount: 99.99,
        status: 'PENDING'
    };
    const result = await bookings.insertOne(booking, { session });
    
    // Step 2: Log event
    await events.insertOne({
        bookingId: result.insertedId,
        eventType: 'BOOKING_CREATED'
    }, { session });
    
    // Both succeed or both rollback
});
```

#### 2. **Event Emission (After Transaction)**

```javascript
// Only send event after local transaction commits
await producer.send({
    topic: 'booking-events',
    messages: [{
        value: JSON.stringify({
            eventType: 'BOOKING_CREATED',
            bookingId: result.insertedId,
            userId: 'user-123',
            amount: 99.99
        })
    }]
});
```

#### 3. **Payment Processing (Local Transaction)**

```javascript
// payment-service/index.js

await session.withTransaction(async () => {
    // Simulate failure for testing
    if (shouldFail) {
        throw new Error('Payment failed');
    }
    
    // Insert payment
    const payment = {
        bookingId: event.bookingId,
        amount: event.amount,
        status: 'COMPLETED'
    };
    await payments.insertOne(payment, { session });
    
    // Log event
    await events.insertOne({
        bookingId: event.bookingId,
        eventType: 'PAYMENT_SUCCESS'
    }, { session });
});
```

#### 4. **Compensation (On Failure)**

```javascript
// booking-service/index.js: Listening to payment-events

if (event.eventType === 'PAYMENT_FAILED') {
    // COMPENSATING TRANSACTION
    await session.withTransaction(async () => {
        // Check current status
        const booking = await bookings.findOne({ _id: event.bookingId });
        
        if (booking && booking.status === 'PENDING') {
            // Rollback: Cancel the booking
            await bookings.updateOne(
                { _id: event.bookingId },
                { $set: { status: 'CANCELLED' } }
            );
        }
    });
}
```

---

## Technology Stack

### Why Each Technology?

| Technology | Purpose | Why This Choice |
|------------|---------|-----------------|
| **Node.js** | Runtime | Lightweight, async, perfect for event-driven systems |
| **Express** | Web Framework | Simple REST APIs, easy to test |
| **MongoDB** | Database | ACID transactions, flexible schema, easy to use |
| **Kafka KRaft** | Message Broker | Event-driven communication, durability, ordering |
| **Docker** | Containerization | Easy setup, consistent environment |

### MongoDB vs SQL for This POC

| Feature | MongoDB | PostgreSQL |
|---------|---------|------------|
| **Transactions** | ✅ Multi-document ACID | ✅ ACID |
| **Schema** | Flexible (NoSQL) | Rigid (SQL) |
| **Scaling** | Horizontal (sharding) | Vertical (harder to scale) |
| **JSON Support** | Native | Via JSONB |
| **Performance** | Fast writes | Fast complex queries |
| **Learning Curve** | Gentle | Steeper |

### Kafka KRaft vs Zookeeper

| Feature | KRaft (Our Choice) | Zookeeper |
|---------|-------------------|-----------|
| **Architecture** | Built-in controller | External coordinator |
| **Setup** | Single container | Multiple containers |
| **Maintenance** | Simpler | Complex |
| **Performance** | Better | Good |
| **Metadata** | Self-managed | External |
| **Use Case** | Modern Kafka | Legacy Kafka |

---

## Setup & Installation

### Prerequisites

```bash
# Required installations
- Docker Desktop (v20.10+)
- Node.js (v16+)
- npm (v8+)
- curl (for testing)

# Verify installations
docker --version        # Docker version 20.10+
node --version         # Node.js version 16+
npm --version          # npm version 8+
```

### Step 1: Clone & Structure

```bash
# Project structure
saga-poc/
├── docker-compose.yml    # Infrastructure setup
├── init-topics.js        # Kafka topic creator
├── test.js               # Automated testing
├── package.json          # Root dependencies
├── booking-service/
│   ├── package.json      # Service dependencies
│   ├── .env              # Configuration
│   └── index.js          # Main service code
└── payment-service/
    ├── package.json      # Service dependencies
    ├── .env              # Configuration
    └── index.js          # Main service code
```

### Step 2: Start Infrastructure

```bash
# 1. Start containers
docker-compose up -d

# Expected output:
# [+] Running 4/4
# ✔ Container kafka            Started
# ✔ Container mongo-booking    Started
# ✔ Container mongo-payment    Started

# 2. Wait for services to initialize
sleep 20

# 3. Verify containers are running
docker ps

# 4. Create Kafka topics
npm install
npm run init-topics

# Expected output:
# 📝 Creating Kafka topics...
# ✅ Connected to Kafka
# 📝 Creating topic: booking-events
# ✅ Topic booking-events created
# 📝 Creating topic: payment-events
# ✅ Topic payment-events created
# ✅ All topics ready!
```

### Step 3: Install Dependencies

```bash
# Booking Service
cd booking-service
npm install
# Installing: express mongodb kafkajs dotenv cors

# Payment Service (in new terminal)
cd ../payment-service
npm install
# Installing: express mongodb kafkajs dotenv cors

# Back to root
cd ..
```

### Step 4: Start Services

```bash
# TERMINAL 1 - Booking Service
cd booking-service
npm start

# Expected output:
# ✅ Connected to MongoDB
# ✅ Booking Service connected to Kafka (KRaft)
# 👂 Listening to payment-events
# 🚀 Booking Service running on port 3001
# 📊 Endpoints:
#   POST   /api/bookings - Create booking
#   GET    /api/bookings/:id - Get booking
#   GET    /api/bookings - List all bookings
#   GET    /api/bookings/:id/events - Get booking events

# TERMINAL 2 - Payment Service
cd payment-service
npm start

# Expected output:
# ✅ Connected to MongoDB
# ✅ Payment Service connected to Kafka (KRaft)
# 👂 Listening to booking-events
# 🚀 Payment Service running on port 3002
# 📊 Endpoints:
#   POST   /api/payments/toggle-failure - Force next payment to fail
#   POST   /api/payments/failure-rate - Set failure rate (0-1)
#   GET    /api/payments/:bookingId - Get payment by booking
#   GET    /api/payments - List all payments
# ⚠️  Failure rate: 40%
```

### Step 5: Verify Everything Works

```bash
# Terminal 3 - Quick test
curl http://localhost:3001/api/bookings
# Expected: {"success":true,"bookings":[]}

curl http://localhost:3002/api/payments
# Expected: {"success":true,"payments":[]}
```

---

## Testing the Flow

### 1. Success Scenario

```bash
# Create a booking (40% chance of failure automatically)
curl -X POST http://localhost:3001/api/bookings \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","amount":99.99}'

# Response:
# {
#   "success": true,
#   "message": "Booking created successfully",
#   "bookingId": "665f5a1b2c3d4e5f6a7b8c9d",
#   "status": "PENDING"
# }

# Wait 2-3 seconds for processing
sleep 3

# Check booking status
curl http://localhost:3001/api/bookings/665f5a1b2c3d4e5f6a7b8c9d

# If payment succeeded:
# {
#   "success": true,
#   "booking": {
#     "_id": "665f5a1b2c3d4e5f6a7b8c9d",
#     "userId": "alice",
#     "amount": 99.99,
#     "status": "CONFIRMED"  ✅
#   }
# }

# If payment failed:
# {
#   "success": true,
#   "booking": {
#     "_id": "665f5a1b2c3d4e5f6a7b8c9d",
#     "userId": "alice",
#     "amount": 99.99,
#     "status": "CANCELLED"  ✅ Compensation worked!
#   }
# }
```

### 2. Force Failure Scenario

```bash
# 1. Force next payment to fail
curl -X POST http://localhost:3002/api/payments/toggle-failure
# Response: {"success":true,"message":"Next payment will fail"}

# 2. Create a booking
curl -X POST http://localhost:3001/api/bookings \
  -H "Content-Type: application/json" \
  -d '{"userId":"bob","amount":49.99}'

# 3. Wait for compensation
sleep 3

# 4. Check status - Should be CANCELLED
curl http://localhost:3001/api/bookings/{bookingId}
# Expected: "status": "CANCELLED"

# 5. Check event history
curl http://localhost:3001/api/bookings/{bookingId}/events
# Expected events:
# [
#   {"eventType": "BOOKING_CREATED"},
#   {"eventType": "BOOKING_CANCELLED"}  ✅ Compensation recorded
# ]
```

### 3. Configure Failure Rate

```bash
# Set 80% failure rate
curl -X POST http://localhost:3002/api/payments/failure-rate \
  -H "Content-Type: application/json" \
  -d '{"rate":0.8}'

# Set 0% failure rate (always success)
curl -X POST http://localhost:3002/api/payments/failure-rate \
  -H "Content-Type: application/json" \
  -d '{"rate":0}'

# Set 100% failure rate (always fail)
curl -X POST http://localhost:3002/api/payments/failure-rate \
  -H "Content-Type: application/json" \
  -d '{"rate":1}'
```

### 4. Automated Testing

```bash
# Run the complete test suite
npm test

# Expected output:
# 🧪 Testing Saga Pattern with MongoDB Transactions
# ================================================
# 
# 📝 Test 1: Creating booking with automatic payment
# ✅ Booking Created: { bookingId: '...' }
# 📊 Final Booking Status: CONFIRMED
# 📋 Events: ['BOOKING_CREATED', 'PAYMENT_SUCCESS']
# 
# 📋 ACID Properties Demonstrated:
#   ✓ Atomicity: MongoDB transaction ensures all-or-nothing
#   ✓ Consistency: Data remains in valid state
#   ✓ Isolation: Transactions are isolated
#   ✓ Durability: Committed changes are persisted
# 
# ==================================================
# 
# 📝 Test 2: Testing compensation with forced failure
# 🔧 Payment failure forced
# ✅ Booking Created: { bookingId: '...' }
# 📊 Final Booking Status: CANCELLED
# ✅ Compensation successful! Booking was cancelled.
# 📋 Events: ['BOOKING_CREATED', 'BOOKING_CANCELLED']
# 💳 Payment Events: ['PAYMENT_FAILED']
```

---

## Understanding the Code

### Key Components Explained

#### 1. **MongoDB Transaction Wrapper**

```javascript
// This pattern ensures ACID within each service
const session = mongoClient.startSession();

try {
    await session.withTransaction(async () => {
        // All operations are atomic
        await collection1.insertOne(data1, { session });
        await collection2.insertOne(data2, { session });
        // If either fails, both rollback
    });
} finally {
    await session.endSession(); // Always close session
}
```

#### 2. **Kafka Producer (Event Emitter)**

```javascript
// Emit event after local transaction commits
await producer.send({
    topic: 'booking-events',
    messages: [{
        key: `booking-${bookingId}`,  // Ensures ordering
        value: JSON.stringify({
            eventType: 'BOOKING_CREATED',
            bookingId: bookingId,
            // ... more data
        })
    }]
});
```

#### 3. **Kafka Consumer (Event Listener)**

```javascript
// Listen for events and process them
await consumer.run({
    eachMessage: async ({ message }) => {
        const event = JSON.parse(message.value.toString());
        
        if (event.eventType === 'BOOKING_CREATED') {
            await processPayment(event);  // Local transaction
        }
        
        if (event.eventType === 'PAYMENT_FAILED') {
            await compensateBooking(event);  // Rollback
        }
    }
});
```

#### 4. **Compensation Logic**

```javascript
// This is the "rollback" in distributed systems
if (event.eventType === 'PAYMENT_FAILED') {
    await session.withTransaction(async () => {
        // Check if still in PENDING state
        const booking = await bookings.findOne(
            { _id: event.bookingId, status: 'PENDING' }
        );
        
        if (booking) {
            // Perform compensation
            await bookings.updateOne(
                { _id: event.bookingId },
                { $set: { status: 'CANCELLED' } }
            );
            
            // Log compensation
            await events.insertOne({
                bookingId: event.bookingId,
                eventType: 'BOOKING_CANCELLED'
            });
        }
    });
}
```

---

## Common Scenarios

### Scenario 1: Everything Works ✅

```
1. POST /api/bookings
   └─> Booking: INSERT (PENDING) ✅
   └─> Event: BOOKING_CREATED 📤

2. Payment Service consumes
   └─> Payment: INSERT (COMPLETED) ✅
   └─> Event: PAYMENT_SUCCESS 📤

3. Booking Service consumes
   └─> Booking: UPDATE (CONFIRMED) ✅

Final State: Booking CONFIRMED, Payment COMPLETED
```

### Scenario 2: Payment Fails (Compensation) 🔄

```
1. POST /api/bookings
   └─> Booking: INSERT (PENDING) ✅
   └─> Event: BOOKING_CREATED 📤

2. Payment Service consumes
   └─> ❌ Payment fails (simulated)
   └─> Event: PAYMENT_FAILED 📤

3. Booking Service consumes
   └─> Compensation: UPDATE (CANCELLED) 🔄

Final State: Booking CANCELLED, No Payment
```

### Scenario 3: Kafka Message Lost (Eventual Consistency)

```
1. POST /api/bookings
   └─> Booking: INSERT (PENDING) ✅
   └─> ❌ Kafka fails (network issue)

2. Booking marked EVENT_FAILED
   └─> Retry mechanism (manual or automated)

3. Resume: Re-send event or manual fix

Final State: Eventual consistency maintained
```

### Scenario 4: Concurrent Transactions (Isolation)

```
Time 1: User A creates booking #100
Time 2: User B creates booking #101
Time 3: Payment for #100 processes
Time 4: Payment for #101 processes

Both transactions are isolated:
- No dirty reads
- No write conflicts
- Each sees consistent snapshot
```

---

## Troubleshooting

### Common Errors & Solutions

#### Error 1: `KafkaJSProtocolError: This server does not host this topic-partition`

**Cause:** Topics not created or Kafka not ready

**Solution:**
```bash
# Ensure Kafka is fully started
docker-compose logs kafka

# Create topics manually
npm run init-topics

# Or wait longer before starting services
sleep 30
```

#### Error 2: `MongoServerError: Transaction numbers are only allowed on a replica set`

**Cause:** MongoDB running in standalone mode without replica set

**Solution:** The default MongoDB in Docker works fine for transactions. If you get this error:

```yaml
# Update docker-compose.yml for MongoDB
mongo-booking:
  image: mongo:6.0
  command: --replSet rs0  # Enable replica set
  # ... rest of config
```

#### Error 3: `ECONNREFUSED: Kafka broker not reachable`

**Cause:** Kafka not running or wrong address

**Solution:**
```bash
# Check Kafka status
docker ps | grep kafka

# Verify Kafka is listening
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# If not running, restart
docker-compose restart kafka
```

#### Error 4: `MongoNetworkError: connect ECONNREFUSED`

**Cause:** MongoDB not running or wrong port

**Solution:**
```bash
# Check MongoDB containers
docker ps | grep mongo

# Verify MongoDB is accessible
docker exec mongo-booking mongosh --eval "db.adminCommand('ping')"

# Check ports
netstat -an | grep 27017
netstat -an | grep 27018
```

#### Error 5: `KafkaJSNumberOfPartitionsNotEqual`

**Cause:** Partition count mismatch

**Solution:**
```bash
# Delete topics and recreate
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic booking-events

docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic payment-events

# Recreate with correct partitions
npm run init-topics
```

### Debugging Tips

#### 1. Check Logs

```bash
# Service logs
docker logs kafka
docker logs mongo-booking
docker logs mongo-payment

# Application logs
# Check terminal where services are running
# Look for:
# ✅ - Success messages
# ❌ - Error messages
# 📤 - Events being sent
# 📨 - Events being received
# 🔄 - Compensation actions
```

#### 2. Check Database Data

```bash
# Connect to booking MongoDB
docker exec -it mongo-booking mongosh -u admin -p admin123

# In MongoDB shell
use booking_db
db.bookings.find().pretty()
db.booking_events.find().pretty()

# Connect to payment MongoDB
docker exec -it mongo-payment mongosh -u admin -p admin123

# In MongoDB shell
use payment_db
db.payments.find().pretty()
db.payment_events.find().pretty()
```

#### 3. Check Kafka Topics

```bash
# List all topics
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Describe topic
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic booking-events

# Consume messages (debug)
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic booking-events --from-beginning
```

#### 4. Test APIs Manually

```bash
# Create booking with timestamp
curl -X POST http://localhost:3001/api/bookings \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"test-$(date +%s)\",\"amount\":99.99}"

# Force failure and test compensation
curl -X POST http://localhost:3002/api/payments/toggle-failure
curl -X POST http://localhost:3001/api/bookings \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"fail-$(date +%s)\",\"amount\":49.99}"

# Check event timeline
curl http://localhost:3001/api/bookings/{id}/events
```

---

## Performance & Scaling

### Current Configuration

| Component | Specification | Reason |
|-----------|---------------|--------|
| **Kafka** | 1 partition, 1 replica | Simplicity for POC |
| **MongoDB** | Single instance | Ease of setup |
| **Services** | Single instance | Focus on pattern |
| **Transactions** | 5-second timeout | Balance performance |

### Production Recommendations

```yaml
# For Production
Kafka:
  - Multiple partitions (based on throughput)
  - Replication factor 3 (high availability)
  - Compression enabled (lower network)

MongoDB:
  - Replica set (3 nodes)
  - Read preference: secondary (read scaling)
  - Write concern: majority (durability)

Services:
  - Horizontal scaling (multiple instances)
  - Load balancer in front
  - Health checks

Monitoring:
  - Kafka: Prometheus + Grafana
  - MongoDB: Ops Manager
  - Application: ELK stack
```

### Scaling Strategies

1. **Vertical Scaling:** Increase resources
   - More CPU, RAM, disk I/O
   
2. **Horizontal Scaling:** More instances
   - Load balancer distributes requests
   - Kafka partitions for parallelism

3. **Database Optimization:**
   - Indexes on frequently queried fields
   - Sharding for large datasets
   - Connection pooling

4. **Event Processing:**
   - Multiple consumer groups
   - Batch processing for high volume
   - Async processing where possible

---

## Security Considerations

### Current Implementation (POC)

```javascript
// Basic authentication (not implemented)
// CORS enabled for testing
// No SSL/TLS
// No rate limiting
// No input validation (basic)
```

### Production Security

```javascript
// 1. Authentication
const auth = require('jsonwebtoken');
app.use((req, res, next) => {
    const token = req.headers.authorization;
    if (!token) return res.status(401).json({ error: 'Unauthorized' });
    // Validate token
});

// 2. Encryption
const crypto = require('crypto');
// Use HTTPS/TLS
// Encrypt sensitive data (PII, payment info)

// 3. Rate Limiting
const rateLimit = require('express-rate-limit');
app.use(rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100 // limit each IP to 100 requests
}));

// 4. Input Validation
const Joi = require('joi');
const schema = Joi.object({
    userId: Joi.string().required(),
    amount: Joi.number().positive().required()
});
```

---

## Best Practices Summary

### ✅ Do's

1. **Use Idempotency Keys**
   ```javascript
   // Prevent duplicate processing
   const idempotencyKey = req.headers['idempotency-key'];
   // Check if already processed
   ```

2. **Implement Retry Logic**
   ```javascript
   // Retry on transient failures
   for (let i = 0; i < 3; i++) {
       try { await sendEvent(); break; }
       catch (e) { await sleep(1000 * i); }
   }
   ```

3. **Log Everything**
   ```javascript
   console.log(`📤 Event sent: ${eventType} for ${bookingId}`);
   console.log(`📨 Event received: ${eventType} for ${bookingId}`);
   console.log(`🔄 Compensation: ${action} for ${bookingId}`);
   ```

4. **Use Correlation IDs**
   ```javascript
   const correlationId = uuid.v4();
   // Pass through all services
   // Helps trace requests across services
   ```

### ❌ Don'ts

1. **Don't Mix Transactional and Non-Transactional Operations**
   - Emit events AFTER transaction commits
   - Don't do I/O inside transactions

2. **Don't Ignore Failures**
   - Handle all errors
   - Have fallback mechanisms

3. **Don't Make Long-Running Transactions**
   - Keep transactions short
   - Use Saga for long operations

4. **Don't Rely on Two-Phase Commit**
   - 2PC is blocking and complex
   - Saga is more scalable

---

## Conclusion

### What We Learned

1. **Distributed Transactions are Hard**
   - SQL transactions don't work across services
   - Need special patterns (Saga, 2PC, etc.)

2. **Saga Pattern Works Well**
   - Compensating transactions provide rollback
   - Event-driven communication is natural
   - Scales better than 2PC

3. **ACID is Achievable**
   - Local transactions give ACID
   - Saga gives eventual consistency
   - Together they solve the problem

4. **Technology Choices Matter**
   - MongoDB for flexible schema + transactions
   - Kafka KRaft for simple setup
   - Node.js for async event handling

### Key Takeaways

```
✅ Each service maintains its own ACID transactions
✅ Events coordinate the overall workflow
✅ Compensation handles failures gracefully
✅ Eventual consistency is acceptable
✅ System remains available and scalable
```

### Further Reading

- [Saga Pattern by Chris Richardson](https://microservices.io/patterns/data/saga.html)
- [MongoDB Transactions](https://docs.mongodb.com/manual/core/transactions/)
- [Kafka KRaft Mode](https://kafka.apache.org/documentation/#kraft)
- [Event-Driven Architecture](https://martinfowler.com/articles/201701-event-driven.html)

---

## Quick Reference Commands

```bash
# Start everything
docker-compose up -d && npm run init-topics
cd booking-service && npm start
cd ../payment-service && npm start

# Test
curl -X POST http://localhost:3001/api/bookings \
  -H "Content-Type: application/json" \
  -d '{"userId":"test","amount":99.99}'

# Force failure
curl -X POST http://localhost:3002/api/payments/toggle-failure

# Check status
curl http://localhost:3001/api/bookings

# Clean up
docker-compose down -v

# Full reset
docker-compose down -v && docker-compose up -d
```

---

# Complete Business Flow: Ticket Booking with Payment

## Business Scenario

Let's understand the **real-world business flow** of booking a movie/event ticket:

```
Customer wants to book a ticket
    ↓
1. Create Booking (Status: PENDING)
    ↓
2. Process Payment
    ↓
3. If Payment SUCCESS → Booking CONFIRMED (Ticket issued)
   If Payment FAILED   → Booking CANCELLED (No ticket)
```

## Simple Explanation of the Flow

### The Two APIs You Need:

1. **One API to Book Ticket** → `POST /api/bookings`
   - This creates the booking
   - Triggers payment automatically
   - You don't need to call payment separately

2. **One API to Check Status** → `GET /api/bookings/:id`
   - Check if booking is CONFIRMED or CANCELLED

### Why Only One API?

In this microservices architecture:

```
You (Client) → Booking Service API
                      ↓
              Booking Service does:
              1. Creates booking (PENDING)
              2. Sends event to Kafka
                      ↓
              Payment Service (automatically):
              3. Processes payment
              4. Sends result back
                      ↓
              Booking Service (automatically):
              5. Updates booking to CONFIRMED or CANCELLED
```

**You only call ONE API** - the rest happens automatically!

---

## Complete Business Flow Explanation

### 1. Success Flow (Happy Path)

```
STEP 1: User makes a booking request
┌─────────────────────────────────────────────────────────────┐
│  POST /api/bookings                                        │
│  {                                                         │
│    "userId": "john_doe",                                   │
│    "amount": 99.99                                         │
│  }                                                         │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 2: Booking Service starts MongoDB Transaction
┌─────────────────────────────────────────────────────────────┐
│  BEGIN TRANSACTION                                         │
│  ├── Insert booking with status: "PENDING"                 │
│  └── Log event: "BOOKING_CREATED"                          │
│  COMMIT TRANSACTION ✅                                     │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 3: Booking Service emits event to Kafka
┌─────────────────────────────────────────────────────────────┐
│  Send to Kafka Topic: "booking-events"                     │
│  Message: {                                                │
│    "eventType": "BOOKING_CREATED",                         │
│    "bookingId": "123",                                     │
│    "userId": "john_doe",                                   │
│    "amount": 99.99                                         │
│  }                                                         │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 4: Payment Service automatically receives the event
┌─────────────────────────────────────────────────────────────┐
│  Payment Service listens to "booking-events" topic         │
│  Received: BOOKING_CREATED for booking 123                 │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 5: Payment Service processes payment
┌─────────────────────────────────────────────────────────────┐
│  BEGIN TRANSACTION                                         │
│  ├── Check if payment should succeed (random 60% success)  │
│  ├── Insert payment with status: "COMPLETED" ✅            │
│  └── Log event: "PAYMENT_SUCCESS"                          │
│  COMMIT TRANSACTION ✅                                     │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 6: Payment Service emits result
┌─────────────────────────────────────────────────────────────┐
│  Send to Kafka Topic: "payment-events"                     │
│  Message: {                                                │
│    "eventType": "PAYMENT_SUCCESS",                         │
│    "bookingId": "123",                                     │
│    "paymentId": "456"                                      │
│  }                                                         │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 7: Booking Service updates status
┌─────────────────────────────────────────────────────────────┐
│  BEGIN TRANSACTION                                         │
│  └── Update booking 123: status = "CONFIRMED" ✅           │
│  COMMIT TRANSACTION ✅                                     │
└─────────────────────────────────────────────────────────────┘

✅ FINAL RESULT: Booking CONFIRMED, Ticket issued!
```

### 2. Failure Flow (Compensation)

```
STEP 1: User makes a booking request
┌─────────────────────────────────────────────────────────────┐
│  POST /api/bookings                                        │
│  {                                                         │
│    "userId": "jane_doe",                                   │
│    "amount": 49.99                                         │
│  }                                                         │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 2: Booking Service creates booking (PENDING)
┌─────────────────────────────────────────────────────────────┐
│  BEGIN TRANSACTION                                         │
│  ├── Insert booking with status: "PENDING"                 │
│  └── Log event: "BOOKING_CREATED"                          │
│  COMMIT TRANSACTION ✅                                     │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 3: Booking Service emits event to Kafka
┌─────────────────────────────────────────────────────────────┐
│  Send to Kafka Topic: "booking-events"                     │
│  Message: BOOKING_CREATED for booking 124                  │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 4: Payment Service receives the event
┌─────────────────────────────────────────────────────────────┐
│  Payment Service listens to "booking-events" topic         │
│  Received: BOOKING_CREATED for booking 124                 │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 5: Payment Service tries to process payment - FAILS! ❌
┌─────────────────────────────────────────────────────────────┐
│  BEGIN TRANSACTION                                         │
│  ├── Simulated failure (or real payment error)             │
│  └── ❌ ERROR: Payment processing failed                    │
│  ROLLBACK TRANSACTION 🔄                                  │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 6: Payment Service emits failure event
┌─────────────────────────────────────────────────────────────┐
│  Send to Kafka Topic: "payment-events"                     │
│  Message: {                                                │
│    "eventType": "PAYMENT_FAILED",                          │
│    "bookingId": "124",                                     │
│    "error": "Payment processing failed"                    │
│  }                                                         │
└─────────────────────────────────────────────────────────────┘
                    ↓
STEP 7: Booking Service receives failure - COMPENSATION!
┌─────────────────────────────────────────────────────────────┐
│  Booking Service listens to "payment-events" topic         │
│  Received: PAYMENT_FAILED for booking 124                  │
│                                                             │
│  💡 COMPENSATION (Rollback):                                │
│  BEGIN TRANSACTION                                         │
│  ├── Check booking 124 is still "PENDING"                  │
│  └── Update booking 124: status = "CANCELLED" 🔄           │
│  COMMIT TRANSACTION ✅                                     │
└─────────────────────────────────────────────────────────────┘

✅ FINAL RESULT: Booking CANCELLED, No ticket issued!
```

---

## Key Takeaways - Simple Summary

### What You Need to Know:

1. **Only ONE API Call** 
   - You just call `POST /api/bookings`
   - Everything else happens automatically

2. **Status Changes Automatically**
   - `PENDING` → Wait for payment
   - `CONFIRMED` → Payment succeeded (Ticket issued)
   - `CANCELLED` → Payment failed (No ticket)

3. **Why Rollback Happens**
   - Payment failure triggers compensation
   - Booking is automatically cancelled
   - No manual intervention needed

4. **See It in MongoDB UI**
   - Booking DB: http://localhost:8081
   - Payment DB: http://localhost:8082
   - Watch status change in real-time

5. **ACID is Maintained**
   - **A**tomicity: All or nothing per service
   - **C**onsistency: Valid booking states only
   - **I**solation: Transactions don't interfere
   - **D**urability: Data persists after commit

### Visual Flow Summary:

```
📱 USER CALLS: POST /api/bookings
         ↓
    [BOOKING PENDING] ← MongoDB Transaction
         ↓
    📤 EVENT → Kafka
         ↓
    💳 PAYMENT SERVICE
         ↓
    ✅ SUCCESS              ❌ FAILURE
         ↓                      ↓
    📤 PAYMENT_SUCCESS      📤 PAYMENT_FAILED
         ↓                      ↓
    [BOOKING CONFIRMED]    [BOOKING CANCELLED] ← Compensation
         ↓                      ↓
    🎫 TICKET ISSUED!       ❌ NO TICKET
```

### Test Commands to See Both Cases:

```bash
# To force success (disable failure)
curl -X POST http://localhost:3002/api/payments/failure-rate -H "Content-Type: application/json" -d '{"rate":0}'

# Create booking (will succeed)
curl -X POST http://localhost:3001/api/bookings -H "Content-Type: application/json" -d '{"userId":"success_user","amount":99.99}'

# Force failure
curl -X POST http://localhost:3002/api/payments/toggle-failure

# Create booking (will fail and rollback)
curl -X POST http://localhost:3001/api/bookings -H "Content-Type: application/json" -d '{"userId":"fail_user","amount":49.99}'
```

---

## Business Logic Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    TICKET BOOKING PROCESS                       │
└─────────────────────────────────────────────────────────────────┘

              ┌──────────────────────────────┐
              │  Customer wants to book a    │
              │  ticket for a movie/event    │
              └────────────┬─────────────────┘
                          │
                          ▼
              ┌──────────────────────────────┐
              │  POST /api/bookings          │
              │  { userId, amount }          │
              └────────────┬─────────────────┘
                          │
                          ▼
              ┌──────────────────────────────┐
              │  Booking Service:            │
              │  Create booking (PENDING)    │
              │  MongoDB Transaction         │
              └────────────┬─────────────────┘
                          │
                          ▼
              ┌──────────────────────────────┐
              │  Send "BOOKING_CREATED"      │
              │  event to Kafka              │
              └────────────┬─────────────────┘
                          │
                          ▼
              ┌──────────────────────────────┐
              │  Payment Service:            │
              │  Process payment             │
              └────┬──────────────────┬──────┘
                   │                  │
            ┌──────▼──────┐   ┌───────▼──────┐
            │  SUCCESS    │   │  FAILED      │
            │  (60%)      │   │  (40%)       │
            └──────┬──────┘   └───────┬──────┘
                   │                  │
                   ▼                  ▼
    ┌─────────────────────┐ ┌─────────────────────┐
    │ Payment SUCCESS     │ │ Payment FAILED      │
    │ Emit PAYMENT_SUCCESS│ │ Emit PAYMENT_FAILED │
    └─────────┬───────────┘ └─────────┬───────────┘
              │                       │
              ▼                       ▼
    ┌─────────────────────┐ ┌─────────────────────┐
    │ Booking CONFIRMED   │ │ Booking CANCELLED   │
    │ ✅ Ticket issued!   │ │ ❌ No ticket!       │
    │ Status: CONFIRMED   │ │ Status: CANCELLED   │
    │ Payment recorded    │ │ Compensation done   │
    └─────────────────────┘ └─────────────────────┘

                    ▼
           ┌────────────────────┐
           │  Customer checks   │
           │  ticket status     │
           │  GET /api/bookings │
           └────────────────────┘
```

---

## Summary - In Simple Words

1. **You only make ONE API call** to book a ticket
2. **Booking starts as PENDING** (waiting for payment)
3. **Payment happens automatically** in the background
4. **If payment succeeds** → Booking becomes CONFIRMED (Ticket issued)
5. **If payment fails** → Booking becomes CANCELLED (Compensation)
6. **You can watch it all** in MongoDB UI (http://localhost:8081)
7. **ACID is maintained** through MongoDB transactions

The system handles the entire business flow automatically!

# replica run command:
# 1. Stop everything
docker-compose down -v

# 2. Start fresh
docker-compose up -d

# 3. Wait for MongoDB
sleep 20

# 4. Initialize replica sets (FIXED with authentication)
chmod +x init-replicas.sh
./init-replicas.sh

# 5. Create Kafka topics
npm run init-topics

# 6. Start services
# Terminal 1:
cd booking-service && npm start

# Terminal 2:
cd payment-service && npm start

1. First, Check if Bookings are Being Created
bash
# Get all bookings via API
curl http://localhost:3001/api/bookings | jq '.'

2. Check Payment Service Status
bash
# Check payment service is running
curl http://localhost:3002/api/payments | jq '.'


# CORRECT TRANSACTION FLOW                                                                                  -->*IMPORTANT NOTES*
--------------------------------------------
1. Start Session
2. Insert Booking (PENDING) - IN TRANSACTION
3. Emit Kafka event (but still in transaction)
4. WAIT for payment result
5. If SUCCESS → COMMIT (Booking saved to DB)
6. If FAILED → ABORT/ROLLBACK (Booking never saved)

## How It Works Now
Success Flow:
------------
1. POST /api/bookings
   → Session STARTED
   → Transaction STARTED
   → Booking inserted (IN TRANSACTION - NOT COMMITTED)
   → Kafka event sent
   → Response: "Waiting for payment confirmation"
   
2. Payment SUCCESS
   → CommitTransaction() ✅
   → Booking SAVED to database
   → Status updated to CONFIRMED

3. Booking is now visible in DB ✅
Failure Flow:
------------
1. POST /api/bookings
   → Session STARTED
   → Transaction STARTED
   → Booking inserted (IN TRANSACTION - NOT COMMITTED)
   → Kafka event sent
   → Response: "Waiting for payment confirmation"
   
2. Payment FAILED
   → AbortTransaction() ❌
   → Booking NEVER saved to database
   → Complete rollback

3. Booking NOT visible in DB ❌

# success db data:
-------------------
keerthana@Mac-2 saga-poc % curl http://localhost:3001/api/bookings | jq '.'
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   260  100   260    0     0  59306      0 --:--:-- --:--:-- --:--:-- 65000
{
  "success": true,
  "bookings": [
    {
      "_id": "6a58a34f72440d33899986d8",
      "userId": "test_user",
      "amount": 99.99,
      "status": "CONFIRMED",
      "createdAt": "2026-07-16T09:24:31.769Z",
      "updatedAt": "2026-07-16T09:24:31.859Z",
      "paymentAmount": 99.99,
      "paymentId": "6a58a34f5dd2e466e43c9745"
    }
  ]
}
keerthana@Mac-2 saga-poc % curl http://localhost:3002/api/payments | jq '.'
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   238  100   238    0     0  20427      0 --:--:-- --:--:-- --:--:-- 21636
{
  "success": true,
  "payments": [
    {
      "_id": "6a58a34f5dd2e466e43c9745",
      "bookingId": "6a58a34f72440d33899986d8",
      "userId": "test_user",
      "amount": 99.99,
      "status": "COMPLETED",
      "createdAt": "2026-07-16T09:24:31.826Z",
      "updatedAt": "2026-07-16T09:24:31.826Z"
    }
  ]
}
# failed db data - data not inserted:
-------------------
keerthana@Mac-2 saga-poc % curl http://localhost:3001/api/bookings | jq '.'
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   260  100   260    0     0  50524      0 --:--:-- --:--:-- --:--:-- 52000
{
  "success": true,
  "bookings": [
    {
      "_id": "6a58a34f72440d33899986d8",
      "userId": "test_user",
      "amount": 99.99,
      "status": "CONFIRMED",
      "createdAt": "2026-07-16T09:24:31.769Z",
      "updatedAt": "2026-07-16T09:24:31.859Z",
      "paymentAmount": 99.99,
      "paymentId": "6a58a34f5dd2e466e43c9745"
    }
  ]
}
keerthana@Mac-2 saga-poc % curl http://localhost:3002/api/payments | jq '.'
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   238  100   238    0     0  54103      0 --:--:-- --:--:-- --:--:-- 59500
{
  "success": true,
  "payments": [
    {
      "_id": "6a58a34f5dd2e466e43c9745",
      "bookingId": "6a58a34f72440d33899986d8",
      "userId": "test_user",
      "amount": 99.99,
      "status": "COMPLETED",
      "createdAt": "2026-07-16T09:24:31.826Z",
      "updatedAt": "2026-07-16T09:24:31.826Z"
    }
  ]
}

# To check the mongodb ui log:
docker logs mongo-express

