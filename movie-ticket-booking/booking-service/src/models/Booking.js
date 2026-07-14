const mongoose = require("mongoose");

const bookingSchema = new mongoose.Schema(
  {
    movieName: {
      type: String,
      required: true,
    },
    customerName: {
      type: String,
      required: true,
    },
    seats: {
      type: Number,
      required: true,
    },
    amount: {
      type: Number,
      required: true,
    },
    status: {
      type: String,
      enum: [
        "PENDING",
        "CONFIRMED",
        "CANCELLED"
      ],
      default: "PENDING",
    },
  },
  {
    timestamps: true,
  }
);

module.exports = mongoose.model("Booking", bookingSchema);

// Why PENDING?
// This is very important.

// When a customer requests a booking:
// Booking Created
// ↓
// Payment Not Done Yet
// ↓
// Seat Not Reserved Yet

// So the booking cannot be marked CONFIRMED.

// Later:
// Payment Success
// ↓
// Seat Reserved
// ↓
// Booking = CONFIRMED

// If anything fails:
// Booking = CANCELLED