const mongoose = require("mongoose");

const seatSchema = new mongoose.Schema(
  {
    bookingId: {
      type: String,
      required: true,
    },
    seatNumber: {
      type: String,
      required: true,
    },
    status: {
      type: String,
      enum: ["RESERVED", "FAILED"],
      default: "RESERVED",
    },
  },
  {
    timestamps: true,
  }
);

module.exports = mongoose.model("Seat", seatSchema);