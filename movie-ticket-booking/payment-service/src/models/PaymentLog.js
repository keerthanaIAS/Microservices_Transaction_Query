const mongoose = require("mongoose");

const paymentLogSchema = new mongoose.Schema(
    {
        paymentId: {
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

module.exports = mongoose.model("PaymentLog", paymentLogSchema);