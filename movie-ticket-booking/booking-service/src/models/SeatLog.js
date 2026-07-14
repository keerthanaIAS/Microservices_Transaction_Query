const mongoose = require("mongoose");

const seatLogSchema = new mongoose.Schema(
    {
        seatId: {
            type: String,
            required: true
        },
        bookingId: {
            type: String,
            required: true
        },
        action: {
            type: String,
            required: true
        }
    },
    {
        timestamps: true
    }
);

module.exports = mongoose.model("SeatLog", seatLogSchema);