const { Kafka, Partitioners } = require('kafkajs');

async function createTopics() {
    const kafka = new Kafka({
        clientId: 'topic-initializer',
        brokers: ['localhost:9092'],
        createPartitioner: Partitioners.LegacyPartitioner
    });

    const admin = kafka.admin();

    try {
        await admin.connect();
        console.log('✅ Connected to Kafka');

        const topics = ['booking-events', 'payment-events'];
        
        for (const topic of topics) {
            try {
                await admin.createTopics({
                    topics: [{
                        topic: topic,
                        numPartitions: 1,
                        replicationFactor: 1,
                    }],
                    waitForLeaders: true,
                });
                console.log(`✅ Topic ${topic} created`);
            } catch (error) {
                if (error.message.includes('already exists')) {
                    console.log(`ℹ️ Topic ${topic} already exists`);
                } else {
                    throw error;
                }
            }
        }
        
        console.log('✅ All topics ready!');
        await admin.disconnect();
    } catch (error) {
        console.error('❌ Failed:', error);
        process.exit(1);
    }
}

createTopics();