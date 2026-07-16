#!/bin/bash

echo "🚀 COMPLETE SETUP STARTING..."
echo "================================"

# Remove only the conflicting mongo-express container
echo "🗑 Removing conflicting mongo-express container..."
docker rm -f mongo-express 2>/dev/null || true

# Stop compose project
echo "🛑 Stopping existing compose project..."
docker compose down

# Start containers
echo "🚀 Starting containers..."
docker compose up -d

# Wait for MongoDB & Kafka
echo "⏳ Waiting for services to start..."
sleep 20

# Initialize replica sets
echo "⚙ Initializing MongoDB Replica Sets..."
chmod +x init-replicas.sh
./init-replicas.sh

# Create Kafka topics
echo "📨 Creating Kafka Topics..."
npm run init-topics

# Show running containers
echo ""
echo "✅ SETUP COMPLETE!"
echo "================================"
echo ""
echo "📊 Running Containers:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "🌐 Mongo Express:"
echo "DB : http://localhost:8081"
echo ""
echo "Login:"
echo "Username : admin"
echo "Password : admin123"

echo ""
echo "🚀 Start Services:"
echo "Terminal 1:"
echo "cd booking-service && npm start"
echo ""
echo "Terminal 2:"
echo "cd payment-service && npm start"

echo ""
echo "🎉 Ready to use!"