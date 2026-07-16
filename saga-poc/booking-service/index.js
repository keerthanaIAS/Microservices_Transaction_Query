const express = require('express');
const { MongoClient, ObjectId } = require('mongodb');
const { Kafka, Partitioners } = require('kafkajs');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(cors());

const mongoClient = new MongoClient(process.env.MONGODB_URI || 'mongodb://localhost:27017');
let db;

const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'booking-service',
    brokers: [(process.env.KAFKA_BROKER || 'localhost:9092')],
    createPartitioner: Partitioners.LegacyPartitioner
});

const producer = kafka.producer({ allowAutoTopicCreation: true });
const consumer = kafka.consumer({ 
    groupId: process.env.KAFKA_GROUP_ID || 'booking-group',
    allowAutoTopicCreation: true
});

// Store pending transactions
const pendingTransactions = new Map();

async function connectKafka() {
    await producer.connect();
    await consumer.connect();
    console.log('✅ Booking Service connected to Kafka');
}

// 📌 CREATE BOOKING - Transaction stays OPEN until payment
app.post('/api/bookings', async (req, res) => {
    const { userId, amount } = req.body;
    const session = mongoClient.startSession();
    let bookingId = null;
    
    try {
        console.log(`🔵 Session STARTED for user ${userId}`);
        
        // Start transaction
        session.startTransaction();
        console.log(`🔵 Transaction STARTED for booking`);
        
        const bookings = db.collection('bookings');
        
        // 1. Insert booking with PENDING status (IN TRANSACTION - NOT COMMITTED YET)
        const booking = {
            userId,
            amount: parseFloat(amount),
            status: 'PENDING',
            createdAt: new Date(),
            updatedAt: new Date()
        };
        
        const result = await bookings.insertOne(booking, { session });
        bookingId = result.insertedId;
        
        console.log(`📝 Booking inserted: ${bookingId} (PENDING) - IN TRANSACTION (NOT COMMITTED)`);
        
        // 2. Log event (inside transaction)
        await db.collection('events').insertOne({
            bookingId,
            eventType: 'BOOKING_CREATED',
            createdAt: new Date()
        }, { session });
        
        console.log(`📝 Event logged: BOOKING_CREATED - IN TRANSACTION (NOT COMMITTED)`);
        
        // 3. Store session for later commit/abort
        pendingTransactions.set(bookingId.toString(), {
            session,
            bookingId,
            userId,
            amount,
            created: new Date()
        });
        
        // 4. Emit Kafka event (transaction is still OPEN)
        await producer.send({
            topic: 'booking-events',
            messages: [{
                key: `booking-${bookingId}`,
                value: JSON.stringify({
                    eventType: 'BOOKING_CREATED',
                    bookingId: bookingId.toString(),
                    userId: userId,
                    amount: parseFloat(amount),
                    timestamp: new Date().toISOString()
                })
            }]
        });
        console.log(`📤 BookingCreated event emitted for ${bookingId}`);
        
        // 5. Return response - Booking is NOT committed yet
        res.status(201).json({
            success: true,
            message: 'Booking created in transaction - Waiting for payment confirmation',
            bookingId: bookingId.toString(),
            status: 'PENDING',
            note: 'Transaction will be committed only after payment success'
        });
        
    } catch (error) {
        // Rollback on error
        if (session) {
            await session.abortTransaction();
            console.log(`❌ Transaction ABORTED (Rollback)`);
        }
        console.error(`❌ Error:`, error.message);
        
        res.status(500).json({
            success: false,
            message: 'Booking creation failed - Transaction rolled back',
            error: error.message
        });
    } finally {
        // Don't end session here - keep it open for payment result
        // Session will be ended after commit or abort
        if (!bookingId || !pendingTransactions.has(bookingId.toString())) {
            await session.endSession();
        }
    }
});

// 📌 LISTEN TO PAYMENT EVENTS - Commit or Abort transaction
async function listenToPaymentEvents() {
    await consumer.subscribe({ topic: 'payment-events', fromBeginning: true });
    
    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const event = JSON.parse(message.value.toString());
                console.log(`📨 Received: ${event.eventType} for booking ${event.bookingId}`);
                
                const bookingId = event.bookingId;
                const pending = pendingTransactions.get(bookingId);
                
                if (!pending) {
                    console.log(`⚠️ No pending transaction found for ${bookingId}`);
                    return;
                }
                
                const { session, bookingId: bId } = pending;
                
                try {
                    if (event.eventType === 'PAYMENT_SUCCESS') {
                        // ✅ PAYMENT SUCCESS - COMMIT the transaction
                        await session.commitTransaction();
                        console.log(`✅ Transaction COMMITTED for booking ${bookingId} (Saved to DB)`);
                        
                        // Update status to CONFIRMED (in a new transaction)
                        const updateSession = mongoClient.startSession();
                        try {
                            await updateSession.withTransaction(async () => {
                                await db.collection('bookings').updateOne(
                                    { _id: new ObjectId(bookingId) },
                                    {
                                        $set: {
                                            status: 'CONFIRMED',
                                            updatedAt: new Date(),
                                            paymentId: event.paymentId,
                                            paymentAmount: event.amount
                                        }
                                    },
                                    { session: updateSession }
                                );
                                
                                await db.collection('events').insertOne({
                                    bookingId: new ObjectId(bookingId),
                                    eventType: 'BOOKING_CONFIRMED',
                                    paymentId: event.paymentId,
                                    createdAt: new Date()
                                }, { session: updateSession });
                            });
                        } finally {
                            await updateSession.endSession();
                        }
                        
                        console.log(`✅ Booking ${bookingId} CONFIRMED`);
                        
                    } else if (event.eventType === 'PAYMENT_FAILED') {
                        // ❌ PAYMENT FAILED - ABORT transaction (Complete Rollback)
                        await session.abortTransaction();
                        console.log(`❌ Transaction ABORTED for booking ${bookingId} (Complete Rollback - Booking never saved)`);
                        
                        // Log the deletion attempt
                        const logSession = mongoClient.startSession();
                        try {
                            await logSession.withTransaction(async () => {
                                await db.collection('events').insertOne({
                                    bookingId: new ObjectId(bookingId),
                                    eventType: 'BOOKING_DELETED',
                                    reason: event.error || 'Payment failed',
                                    createdAt: new Date()
                                }, { session: logSession });
                            });
                        } finally {
                            await logSession.endSession();
                        }
                        
                        console.log(`🗑️ Booking ${bookingId} DELETED (Complete Rollback)`);
                    }
                    
                } catch (error) {
                    console.error(`❌ Error processing payment result:`, error);
                    try {
                        await session.abortTransaction();
                        console.log(`❌ Transaction ABORTED due to error`);
                    } catch (abortError) {
                        console.error(`❌ Failed to abort transaction:`, abortError);
                    }
                } finally {
                    // Clean up: remove from pending and end session
                    pendingTransactions.delete(bookingId);
                    await session.endSession();
                    console.log(`🔵 Session ENDED for booking ${bookingId}`);
                }
                
            } catch (error) {
                console.error('❌ Error processing payment event:', error);
            }
        }
    });
}

// 📌 GET BOOKING BY ID
app.get('/api/bookings/:id', async (req, res) => {
    try {
        const booking = await db.collection('bookings').findOne({
            _id: new ObjectId(req.params.id)
        });
        
        if (!booking) {
            // Check if it's in pending transactions
            const pending = pendingTransactions.get(req.params.id);
            if (pending) {
                return res.status(202).json({
                    success: true,
                    message: 'Booking is in pending transaction - Waiting for payment',
                    bookingId: req.params.id,
                    status: 'PENDING_TRANSACTION',
                    note: 'Booking will be committed or rolled back based on payment result'
                });
            }
            
            return res.status(404).json({ 
                success: false, 
                message: 'Booking not found - It was rolled back due to payment failure' 
            });
        }
        
        booking._id = booking._id.toString();
        res.json({ success: true, booking });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// 📌 GET ALL BOOKINGS (Only committed bookings)
app.get('/api/bookings', async (req, res) => {
    try {
        const bookings = await db.collection('bookings')
            .find()
            .sort({ createdAt: -1 })
            .toArray();
        
        const formatted = bookings.map(b => ({
            ...b,
            _id: b._id.toString()
        }));
        
        res.json({ success: true, bookings: formatted });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// 📌 GET EVENTS
app.get('/api/bookings/:id/events', async (req, res) => {
    try {
        const events = await db.collection('events')
            .find({ bookingId: req.params.id })
            .sort({ createdAt: 1 })
            .toArray();
        
        const formatted = events.map(e => ({
            ...e,
            _id: e._id.toString(),
            bookingId: e.bookingId.toString()
        }));
        
        res.json({ success: true, events: formatted });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// 📌 DEBUG - Get all data
app.get('/api/debug/all', async (req, res) => {
    try {
        const bookings = await db.collection('bookings').find({}).toArray();
        const events = await db.collection('events').find({}).toArray();
        const pending = Array.from(pendingTransactions.keys());
        
        res.json({
            success: true,
            committedBookings: bookings.length,
            bookings: bookings.map(b => ({ ...b, _id: b._id.toString() })),
            pendingTransactions: pending,
            totalEvents: events.length,
            events: events.map(e => ({ ...e, _id: e._id.toString(), bookingId: e.bookingId.toString() }))
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// 📌 START SERVER
async function startServer() {
    try {
        await mongoClient.connect();
        db = mongoClient.db(process.env.MONGODB_DB || 'saga_db');
        console.log('✅ Connected to MongoDB (saga_db)');
        
        await db.collection('bookings').createIndex({ userId: 1 });
        await db.collection('bookings').createIndex({ status: 1 });
        await db.collection('events').createIndex({ bookingId: 1 });
        
        await connectKafka();
        await listenToPaymentEvents();
        
        const port = process.env.PORT || 3001;
        app.listen(port, () => {
            console.log(`🚀 Booking Service running on port ${port}`);
            console.log(`📊 Single Database: saga_db`);
            console.log(`📊 Collection: bookings`);
            console.log(`\n💡 Transaction Flow:`);
            console.log(`   1. Booking created in transaction (NOT committed)`);
            console.log(`   2. Payment processed`);
            console.log(`   3. If SUCCESS → COMMIT (Booking saved to DB)`);
            console.log(`   4. If FAILED → ABORT (Booking never saved)`);
        });
    } catch (error) {
        console.error('❌ Failed to start:', error);
        process.exit(1);
    }
}

startServer();