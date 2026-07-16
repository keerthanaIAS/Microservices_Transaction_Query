const express = require('express');
const { MongoClient } = require('mongodb');
const { Kafka, Partitioners } = require('kafkajs');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(cors());

// Single MongoDB connection
const mongoClient = new MongoClient(process.env.MONGODB_URI || 'mongodb://localhost:27017');
let db;

const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'payment-service',
    brokers: [(process.env.KAFKA_BROKER || 'localhost:9092')],
    createPartitioner: Partitioners.LegacyPartitioner
});

const producer = kafka.producer({ allowAutoTopicCreation: true });
const consumer = kafka.consumer({ 
    groupId: process.env.KAFKA_GROUP_ID || 'payment-group',
    allowAutoTopicCreation: true
});

let forceFailNext = false;
let failureRate = parseFloat(process.env.FAILURE_RATE) || 0.4;

async function connectKafka() {
    await producer.connect();
    await consumer.connect();
    console.log('✅ Payment Service connected to Kafka');
}

// 📌 PROCESS PAYMENT
async function processPayment(event) {
    const { bookingId, userId, amount } = event;
    const session = mongoClient.startSession();
    let paymentId = null;
    
    try {
        const shouldFail = Math.random() < failureRate || forceFailNext;
        
        if (shouldFail) {
            throw new Error('Payment processing failed (simulated)');
        }
        
        // ✅ PAYMENT SUCCESS
        await session.withTransaction(async () => {
            const payments = db.collection('payments');
            
            // Create payment record
            const payment = {
                bookingId: bookingId,
                userId: userId,
                amount: parseFloat(amount),
                status: 'COMPLETED',
                createdAt: new Date(),
                updatedAt: new Date()
            };
            
            const result = await payments.insertOne(payment, { session });
            paymentId = result.insertedId;
            
            console.log(`💳 Payment ${paymentId} completed for booking ${bookingId}`);
            
            // Log payment event
            await db.collection('events').insertOne({
                bookingId: bookingId,
                paymentId: paymentId.toString(),
                eventType: 'PAYMENT_SUCCESS',
                createdAt: new Date()
            }, { session });
        });
        
        forceFailNext = false;
        
        // Emit success event
        await producer.send({
            topic: 'payment-events',
            messages: [{
                key: `payment-${paymentId}`,
                value: JSON.stringify({
                    eventType: 'PAYMENT_SUCCESS',
                    paymentId: paymentId.toString(),
                    bookingId: bookingId,
                    userId: userId,
                    amount: parseFloat(amount),
                    timestamp: new Date().toISOString()
                })
            }]
        });
        
        console.log(`📤 PaymentSuccess event emitted for ${bookingId}`);
        
    } catch (error) {
        // ❌ PAYMENT FAILED
        console.error(`❌ Payment failed for ${bookingId}:`, error.message);
        
        // Emit failure event (Booking service will DELETE the booking)
        await producer.send({
            topic: 'payment-events',
            messages: [{
                key: `payment-failed-${bookingId}`,
                value: JSON.stringify({
                    eventType: 'PAYMENT_FAILED',
                    bookingId: bookingId,
                    userId: userId,
                    amount: parseFloat(amount),
                    error: error.message,
                    timestamp: new Date().toISOString()
                })
            }]
        });
        console.log(`📤 PaymentFailed event emitted for ${bookingId}`);
    } finally {
        await session.endSession();
    }
}

// 📌 LISTEN TO BOOKING EVENTS
async function listenToBookingEvents() {
    await consumer.subscribe({ topic: 'booking-events', fromBeginning: true });
    
    await consumer.run({
        eachMessage: async ({ message }) => {
            try {
                const event = JSON.parse(message.value.toString());
                console.log(`📨 Received: ${event.eventType} for ${event.bookingId}`);
                
                if (event.eventType === 'BOOKING_CREATED') {
                    await processPayment(event);
                }
            } catch (error) {
                console.error('❌ Error:', error);
            }
        }
    });
}

// 📌 API ROUTES
app.post('/api/payments/toggle-failure', (req, res) => {
    forceFailNext = true;
    res.json({ success: true, message: 'Next payment will fail' });
});

app.post('/api/payments/failure-rate', (req, res) => {
    const { rate } = req.body;
    if (typeof rate === 'number' && rate >= 0 && rate <= 1) {
        failureRate = rate;
        res.json({ success: true, failureRate: rate });
    } else {
        res.status(400).json({ success: false, message: 'Rate must be between 0 and 1' });
    }
});

app.get('/api/payments/:bookingId', async (req, res) => {
    try {
        const payment = await db.collection('payments').findOne({
            bookingId: req.params.bookingId
        });
        
        if (!payment) {
            return res.status(404).json({ success: false, message: 'Payment not found' });
        }
        
        payment._id = payment._id.toString();
        res.json({ success: true, payment });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.get('/api/payments', async (req, res) => {
    try {
        const payments = await db.collection('payments')
            .find()
            .sort({ createdAt: -1 })
            .toArray();
        
        const formatted = payments.map(p => ({
            ...p,
            _id: p._id.toString()
        }));
        
        res.json({ success: true, payments: formatted });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// 📌 START SERVER
async function startServer() {
    try {
        await mongoClient.connect();
        db = mongoClient.db(process.env.MONGODB_DB || 'saga_db');
        console.log('✅ Connected to MongoDB (saga_db)');
        
        await db.collection('payments').createIndex({ bookingId: 1 });
        await db.collection('payments').createIndex({ userId: 1 });
        await db.collection('events').createIndex({ bookingId: 1 });
        
        await connectKafka();
        await listenToBookingEvents();
        
        const port = process.env.PORT || 3002;
        app.listen(port, () => {
            console.log(`🚀 Payment Service running on port ${port}`);
            console.log(`⚠️  Failure rate: ${failureRate * 100}%`);
        });
    } catch (error) {
        console.error('❌ Failed to start:', error);
        process.exit(1);
    }
}

startServer();