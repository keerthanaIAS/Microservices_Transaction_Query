const mongoose = require("mongoose");

const bookingLogSchema = new mongoose.Schema(
{
    bookingId: String,
    action: String
},
{
    timestamps:true
});

module.exports = mongoose.model("BookingLog", bookingLogSchema);