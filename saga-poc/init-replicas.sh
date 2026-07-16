#!/bin/bash

echo "⏳ Waiting for MongoDB to start..."
sleep 15

echo "🔧 Initializing replica set for MongoDB..."
docker exec mongo mongosh --eval '
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "localhost:27017" }]
})
'

echo "✅ Replica set initialized!"
sleep 5