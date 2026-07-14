# Step 1: Create the Microservices

## Project Structure

```text
movie-ticket-booking/

├── booking-service/
├── payment-service/
├── seat-service/
├── notification-service/
└── README.md
```

Open a terminal:

```bash
mkdir movie-ticket-booking
cd movie-ticket-booking
```

Create the services:

```bash
mkdir booking-service
mkdir payment-service
mkdir seat-service
mkdir notification-service
```

Your folder should look like:

```text
movie-ticket-booking/

├── booking-service/
├── payment-service/
├── seat-service/
└── notification-service/
```

---

# Step 2: Initialize Every Service

Go into each service and initialize Node.js.

### Booking Service

```bash
cd booking-service
npm init -y
```

Install dependencies:

```bash
npm install express mongoose axios dotenv
npm install --save-dev nodemon
```

Do the same for the remaining services.

### Payment Service

```bash
cd ../payment-service

npm init -y

npm install express mongoose axios dotenv
npm install --save-dev nodemon
```

### Seat Service

```bash
cd ../seat-service

npm init -y

npm install express mongoose axios dotenv
npm install --save-dev nodemon
```

### Notification Service

```bash
cd ../notification-service

npm init -y

npm install express mongoose axios dotenv
npm install --save-dev nodemon
```

---

# Why four microservices?

Each microservice owns one responsibility and one database.

```
Booking Service
        │
 booking_db
```

```
Payment Service
        │
 payment_db
```

```
Seat Service
        │
 seat_db
```

```
Notification Service
        │
 notification_db
```

This is a fundamental microservice principle: **each service owns its data**. Other services communicate through APIs or events—they do not directly access another service's database.

---

# Booking:
---

## Why `PENDING`?

This is very important.

When a customer requests a booking:

```
Booking Created

↓

Payment Not Done Yet

↓

Seat Not Reserved Yet
```

So the booking **cannot** be marked `CONFIRMED`.

Initial state:

```
PENDING
```

Later:

```
Payment Success

↓

Seat Reserved

↓

Booking = CONFIRMED
```

If anything fails:

```
Booking = CANCELLED
```

This status is part of the Saga workflow.

---

MongoDB now contains:

```
booking_db

↓

bookings

↓

status = PENDING
```

---

# Why are we saving before payment?

This often confuses people.

You might think:

> Why not call Payment Service first?

Because every request needs a unique booking record that all later services can refer to.

Imagine this flow:

```
Booking Service

↓

Booking ID = 687ab...

↓

Payment Service

↓

Seat Service
```

Every subsequent service uses the same booking ID to associate its own work with the booking.

---

## Architecture

```
Client

↓

Booking Service

↓

booking_db

Status = PENDING
```

No other microservice is involved yet.

---

# The architecture becomes:

Client
   │
POST /book-ticket
   │
   ▼
Booking Service
   │
   ├── Save Booking (PENDING)
   │
   └── HTTP (Axios)
         │
         ▼
   Payment Service
         │
         └── Save Payment

# Payment flow:
What Happens Internally?

When you send:
POST /api/bookings/book-ticket

the execution flow is:
Booking Service
↓
Create Booking
↓
booking_db
↓
status = PENDING
↓
Axios POST
↓
Payment Service
↓
payment_db
↓
Payment Created
↓
Return success
↓
Booking Status = CONFIRMED
↓
Response to Client

## Alone Booking API & Response:
curl --location 'http://localhost:3001/api/bookings/book-ticket' \
--header 'Content-Type: application/json' \
--data '{
  "movieName": "Interstellar",
  "customerName": "Keerthana",
  "seats": 2,
  "amount": 500
}'
RES:
{
    "movieName": "Interstellar",
    "customerName": "Keerthana",
    "seats": 2,
    "amount": 500,
    "status": "PENDING",
    "_id": "6a55c655de6003334aa31b08",
    "createdAt": "2026-07-14T05:17:09.677Z",
    "updatedAt": "2026-07-14T05:17:09.677Z",
    "__v": 0
}

## After Payment included after booking API & Response:
curl --location 'http://localhost:3001/api/bookings/book-ticket' \
--header 'Content-Type: application/json' \
--data '{
  "movieName": "Interstellar",
  "customerName": "Keerthana",
  "seats": 2,
  "amount": 500
}'
RES:
{
    "booking": {
        "movieName": "Interstellar",
        "customerName": "Keerthana",
        "seats": 2,
        "amount": 500,
        "status": "CONFIRMED",
        "_id": "6a55c98d3e8286469d655d15",
        "createdAt": "2026-07-14T05:30:53.629Z",
        "updatedAt": "2026-07-14T05:30:53.717Z",
        "__v": 0
    },
    "payment": {
        "bookingId": "6a55c98d3e8286469d655d15",
        "amount": 500,
        "status": "SUCCESS",
        "_id": "6a55c98d199ee04a69be1a03",
        "createdAt": "2026-07-14T05:30:53.701Z",
        "updatedAt": "2026-07-14T05:30:53.701Z",
        "__v": 0
    }
}
* You should see:
- A document created in booking_db.bookings
- A document created in payment_db.payments
- Booking status updated from PENDING to CONFIRMED

### Current flow:

Client
   │
   ▼
Booking Service
   │
   ├── Save Booking (PENDING)
   │
   ├── Call Payment Service
   │
   ├── Payment Success
   │
   └── Update Booking → CONFIRMED

#### New Flow

```text
Booking Created
↓
Status = PENDING
↓
Payment Service
↓
Failed ❌
↓
Booking
↓
Status = CANCELLED
```

Now the database becomes:
```text
booking_db
↓
Booking
↓
CANCELLED
```

instead of remaining forever in `PENDING`.

---

# Test

### Case 1

Payment Service running.

Result:

```text
Booking

↓

CONFIRMED
```

---

### Case 2

Payment Service stopped.

Result:

```text
Booking

↓

CANCELLED
```

---

# What implemented?

Many beginners think Saga starts with Kafka.

It doesn't.

You have already implemented the **first part of a Saga**:

```text
Business Step 1
↓
Create Booking
↓
Business Step 2
↓
Process Payment
↓
Failure
↓
Compensating Action
↓
Cancel Booking
```

The compensation here is:
```text
Create Booking
↓
Cancel Booking
```
No MongoDB rollback happened.
Instead, you performed another business operation to restore consistency.

---

# Add Seat Service
Now we have three microservices.
                Client
                   │
                   ▼
          Booking Service
                   │
          ┌────────┴────────┐
          ▼                 ▼
 Payment Service      Seat Service

Business flow:
Create Booking
↓
Payment Success
↓
Reserve Seat
↓
Booking CONFIRMED

Failure flow:
Create Booking
↓
Payment Success
↓
Seat Reservation Failed ❌
↓
Refund Payment
↓
Cancel Booking
- Notice something important.

Previously:
Booking
↓
Payment

Now:
Booking
↓
Payment
↓
Seat
- The Payment Service has already committed its data.

## So if Seat fails, changing the booking to CANCELLED is not enough.
# We need a compensation
Instead of deleting the payment:
Payment
↓
DELETE ❌

We perform another business operation:
SUCCESS
↓
REFUNDED
- That is the compensation.

# Current Flow
Client
   │
   ▼
Booking Service
   │
   ▼
booking_db

## We already changed it to:
Client
   │
   ▼
Booking Service
   │
   ▼
Payment Service

### Now we're extending it to:
Client
   │
   ▼
Booking Service
   │
   ▼
Payment Service
   │
   ▼
Seat Service

#### Just added seats in after booking and payment API & response:
curl --location 'http://localhost:3001/api/bookings/book-ticket' \
--header 'Content-Type: application/json' \
--data '{
  "movieName": "Interstellar",
  "customerName": "Keerthana",
  "seats": 2,
  "amount": 500
}'
RES:
{
    "booking": {
        "movieName": "Interstellar",
        "customerName": "Keerthana",
        "seats": 2,
        "amount": 500,
        "status": "CONFIRMED",
        "_id": "6a55d483117eaaf45a09e669",
        "createdAt": "2026-07-14T06:17:39.785Z",
        "updatedAt": "2026-07-14T06:17:39.848Z",
        "__v": 0
    },
    "payment": {
        "bookingId": "6a55d483117eaaf45a09e669",
        "amount": 500,
        "status": "SUCCESS",
        "_id": "6a55d483506467dfbe43c5bf",
        "createdAt": "2026-07-14T06:17:39.822Z",
        "updatedAt": "2026-07-14T06:17:39.822Z",
        "__v": 0
    },
    "seat": {
        "bookingId": "6a55d483117eaaf45a09e669",
        "seatNumber": "2",
        "status": "RESERVED",
        "_id": "6a55d4838a89400659020c87",
        "createdAt": "2026-07-14T06:17:39.841Z",
        "updatedAt": "2026-07-14T06:17:39.841Z",
        "__v": 0
    }
}

##### Current flow:
Booking Service
      │
      ▼
Create Booking (PENDING)
      │
      ▼
Payment Service
      │
      ▼
SUCCESS
      │
      ▼
Seat Service
      │
      ▼
RESERVED
      │
      ▼
Booking → CONFIRMED

- Now we're going to implement the first real Saga compensation.

###### Refund Payment if Seat Reservation Fails
Goal
-------
If Seat Service fails:
Booking Created
↓
Payment SUCCESS
↓
Seat FAILED ❌
↓
Refund Payment
↓
Booking CANCELLED
- Notice something very important.
- We are not deleting the payment record.

Instead, we're changing:
SUCCESS
↓
REFUNDED
- This is called a compensating transaction.

# Notice:
------------------------- in booking services --------
paymentService.js only knows how to talk to the Payment Service.
seatService.js only knows how to talk to the Seat Service.
bookingSaga.js only coordinates the workflow.

## What we've implemented so far
You now have:
Controller
↓
Booking Saga
↓
Payment Service Wrapper
↓
Seat Service Wrapper
↓
Payment Microservice
↓
Seat Microservice

# Add Notification Service
Let's complete the business workflow.

Current flow:
Booking
   │
   ▼
Payment
   │
   ▼
Seat

New flow:
Booking
   │
   ▼
Payment
   │
   ▼
Seat
   │
   ▼
Notification

If everything succeeds:
Booking Created
      │
      ▼
Payment SUCCESS
      │
      ▼
Seat RESERVED
      │
      ▼
Notification Sent
      │
      ▼
Booking CONFIRMED

If Notification fails:
Booking Created
      │
      ▼
Payment SUCCESS
      │
      ▼
Seat RESERVED
      │
      ▼
Notification FAILED

## After add Notification after the step of booking, payment, seat reserve API & response:
curl --location 'http://localhost:3001/api/bookings/book-ticket' \
--header 'Content-Type: application/json' \
--data '{
  "movieName": "Interstellar",
  "customerName": "Keerthana",
  "seats": 2,
  "amount": 500
}'
RES:
{
    "success": true,
    "booking": {
        "movieName": "Interstellar",
        "customerName": "Keerthana",
        "seats": 2,
        "amount": 500,
        "status": "CONFIRMED",
        "_id": "6a55db5498f6bc5b00ca3150",
        "createdAt": "2026-07-14T06:46:44.285Z",
        "updatedAt": "2026-07-14T06:46:44.424Z",
        "__v": 0
    },
    "payment": {
        "bookingId": "6a55db5498f6bc5b00ca3150",
        "amount": 500,
        "status": "SUCCESS",
        "_id": "6a55db54311398ca4c2f66f0",
        "createdAt": "2026-07-14T06:46:44.303Z",
        "updatedAt": "2026-07-14T06:46:44.303Z",
        "__v": 0
    },
    "seat": {
        "bookingId": "6a55db5498f6bc5b00ca3150",
        "seatNumber": "A10",
        "status": "RESERVED",
        "_id": "6a55db54e69fd635eb52752d",
        "createdAt": "2026-07-14T06:46:44.312Z",
        "updatedAt": "2026-07-14T06:46:44.312Z",
        "__v": 0
    },
    "notification": {
        "bookingId": "6a55db5498f6bc5b00ca3150",
        "message": "Your ticket has been booked successfully.",
        "status": "SENT",
        "_id": "6a55db547925b7cae877bcbe",
        "createdAt": "2026-07-14T06:46:44.412Z",
        "updatedAt": "2026-07-14T06:46:44.412Z",
        "__v": 0
    }
}

# Add the Transaction query:
------------------------------
## MongoDB transactions and Saga.
The architecture will look like this:
Client
   │
   ▼
Booking Service
   │
   ├── MongoDB Transaction (booking_db)
   │       startTransaction()
   │       commit()/abort()
   │
   ▼
Payment Service
   │
   ├── MongoDB Transaction (payment_db)
   │       startTransaction()
   │       commit()/abort()
   │
   ▼
Seat Service
   │
   ├── MongoDB Transaction (seat_db)
   │       startTransaction()
   │       commit()/abort()
   │
   ▼
Notification Service

Notice:
--------------
- Each microservice has its own transaction.
- The Saga coordinates between those transactions.

# By the end
You'll have both concepts working together:

Booking Service
----------------
startTransaction()
Booking
BookingLog
commit()
↓
Saga
↓

Payment Service
----------------
startTransaction()
Payment
PaymentLog
commit()
↓
Saga
↓

Seat Service
----------------
startTransaction()
Seat
SeatInventory
commit()

# Final Architecture
Booking Service
────────────────────────
Booking
BookingLog
Transaction
Commit
↓
Saga
↓

Payment Service
────────────────────────
Payment
PaymentLog
Transaction
Commit
↓
Saga
↓

Seat Service
────────────────────────
Seat
SeatLog
Transaction
Commit
↓
Saga
↓

Notification Service

# completed the HTTP Saga with local MongoDB transactions.

Inside a Microservice
---------------------
startSession()
startTransaction()
CRUD Operations
commitTransaction()
abortTransaction()
endSession()

Across Microservices
--------------------
Booking
   ↓
Payment
   ↓
Seat
   ↓
Notification

If failure:
Refund Payment
Cancel Booking

This is exactly how local ACID transactions and a Saga work together.

# why need replica need transaction only work with replica? why need replica now why i need to mention replcia in yaml file? without this why not working i need reason
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
This is the key concept. Once you understand this, MongoDB transactions will make much more sense.

## Short answer

**Yes. MongoDB multi-document transactions only work on a Replica Set (or a sharded cluster). They do not work on a standalone MongoDB server.**

This is a MongoDB design decision, not a limitation of your code.

---

# Your current setup

Right now you have:

```text
Booking Service
      │
      ▼
Standalone MongoDB
```

Your code does:

```javascript
const session = await mongoose.startSession();
session.startTransaction();
```

MongoDB immediately checks:

```text
Am I running as a Replica Set?
```

Your server answers:

```text
No
```

So MongoDB returns:

```text
Transactions are not supported.
```

---

# Why does MongoDB require a Replica Set?

Think about what a transaction promises.

```text
startTransaction()

↓

Insert Booking

↓

Insert BookingLog

↓

commitTransaction()
```

Until `commitTransaction()`:

* the changes must remain invisible
* MongoDB must remember every change
* MongoDB must be able to undo every change if you call `abortTransaction()`

Now imagine this:

```text
startTransaction()

↓

Insert Booking

↓

Power Failure 💥
```

What should MongoDB do after restart?

It must know:

* Was the transaction committed?
* Was it aborted?
* Should it roll back?
* Should it finish committing?

To answer those questions, MongoDB stores **transaction state** in the replication machinery (the oplog and related infrastructure). That machinery exists only when MongoDB is running as a **replica set**.

A standalone server has no replication subsystem, so it doesn't support the transaction protocol.

---

# Why even a single-node Replica Set?

A common misunderstanding is:

> "Replica Set means multiple servers."

Not necessarily.

You can have:

```text
Replica Set

┌──────────────┐
│ Primary      │
└──────────────┘
```

Just one node.

There are no secondary servers.

But because MongoDB is running in **replica set mode**, it enables:

* sessions
* oplog
* transaction coordinator
* transaction state management
* majority commit logic

That's enough for transactions.

---

# Standalone vs Replica Set

### Standalone

```text
MongoDB

↓

Insert

↓

Done
```

No transaction coordinator.

No oplog.

No transaction support.

---

### Single-node Replica Set

```text
MongoDB

↓

Replica Set Mode

↓

Oplog

↓

Sessions

↓

Transaction Coordinator

↓

Transactions Enabled
```

---

# Why do we add this to Docker?

Instead of starting MongoDB like this:

```bash
mongod
```

we start it like this:

```bash
mongod --replSet rs0
```

That tells MongoDB:

> "Run in replica set mode."

Then we initialize it once:

```javascript
rs.initiate()
```

After that:

```javascript
session.startTransaction()
```

works.

---

# Does replication actually happen with one node?

No.

There is only:

```text
Primary
```

No secondary.

Nothing is copied anywhere.

But MongoDB still enables all the internal transaction infrastructure because it's operating as a replica set.

---

# Why did MongoDB's engineers make this requirement?

Transactions rely on the same mechanisms used for replication:

* **oplog** to record operations in order
* **logical sessions** to track transaction context
* **transaction numbers** to identify transaction operations
* **majority commit point** to determine when a transaction is durably committed

Rather than implement a separate transaction engine for standalone servers, MongoDB built transactions on top of the existing replica set architecture.

---

## So for your learning path

This isn't just because you want to learn replica sets later.

It's because **transactions literally depend on replica set mode**.

# One simple answer                                                                                   -->*important notes*
-------------------------
MongoDB transactions work only on a Replica Set because MongoDB's transaction engine is built on the Replica Set architecture. A standalone MongoDB simply doesn't have that transaction engine enabled.

# The simplest thing to remember
--------------------------------
If you use startTransaction(), MongoDB requires Replica Set mode—even if your replica set has only one node.

## What does "multiple documents" mean?
----------------------------------------
Suppose you do this:
await Booking.create(...);
await BookingLog.create(...);

These are two different MongoDB documents:
Bookings Collection
-------------------
Booking Document

BookingLogs Collection
----------------------
BookingLog Document

- A transaction makes these two writes behave as one unit.

If the second write fails:
Booking Insert ✅
BookingLog Insert ❌
↓
abortTransaction()
↓
Booking Insert is also rolled back ✅

*You need Replica Set mode, because that's where MongoDB enables transactions.*                       --->*important notes*

# Flow Diagram
Booking Request
    │
    ▼
Booking Service (Transaction)
    ├── Create Booking (PENDING)
    └── Create Booking Log
    │
    ▼
Payment Service (Transaction)
    ├── Create Payment (SUCCESS)
    └── Create Payment Log
    │
    ▼
Seat Service (Transaction)
    ├── Check Inventory (Is A10 available?)
    │   ├── YES → Reserve in Inventory
    │   └── NO → abortTransaction() ❌
    ├── Create Seat (RESERVED)
    └── Create Seat Log
    │
    ▼
Booking Service
    ├── If Seat Failed → Refund Payment + Cancel Booking
    └── If Success → Confirm Booking