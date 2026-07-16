# Use Node.js 18 Alpine image (smaller size)
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application code
COPY . .

# Expose port
EXPOSE 3001

# Environment variables
ENV PORT=3001
ENV MONGODB_URI=mongodb://mongo:27017
ENV MONGODB_DB=saga_db
ENV KAFKA_BROKER=kafka:9092
ENV KAFKA_CLIENT_ID=booking-service
ENV KAFKA_GROUP_ID=booking-group

# Start the service
CMD ["npm", "start"]