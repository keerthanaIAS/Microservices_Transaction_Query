const mongoose = require("mongoose");

const notificationSchema = new mongoose.Schema(
{
    bookingId: String,
    message: String,
    status: {
        type: String,
        enum: ["SENT","FAILED"],
        default: "SENT"
    }
},
{
    timestamps:true
});

module.exports = mongoose.model("Notification",notificationSchema);