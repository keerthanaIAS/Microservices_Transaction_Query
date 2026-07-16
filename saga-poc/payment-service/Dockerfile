# Build stage
FROM node:18-alpine AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install all dependencies (including dev)
RUN npm ci

# Copy source code
COPY . .

# Production stage
FROM node:18-alpine

WORKDIR /app

# Copy only necessary files from builder
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/index.js ./

# Expose port
EXPOSE 3002

# Environment variables
ENV PORT=3002
ENV MONGODB_URI=mongodb://mongo:27017
ENV MONGODB_DB=saga_db
ENV KAFKA_BROKER=kafka:9092
ENV KAFKA_CLIENT_ID=payment-service
ENV KAFKA_GROUP_ID=payment-group
ENV FAILURE_RATE=0.4

# Start the service
CMD ["node", "index.js"]