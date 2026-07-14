const mongoose = require("mongoose");
const Booking = require("../models/Booking");
const BookingLog = require("../models/BookingLog");
const { sendNotification } = require("./notificationService");
const {
    processPayment,
    refundPayment
} = require("./paymentService");

const {
    reserveSeat
} = require("./seatService");

const executeBookingSaga = async (bookingData) => {
    const session = await mongoose.startSession();

    let booking;
    let payment;
    let transactionCommitted = false;

    try {
        session.startTransaction();

        const bookings = await Booking.create(
            [bookingData],
            { session }
        );

        booking = bookings[0];
        console.log('booking: ', booking);

        await BookingLog.create(
            [
                {
                    bookingId: booking._id,
                    action: "BOOKING_CREATED"
                }
            ],
            { session }
        );

        await session.commitTransaction();

        transactionCommitted = true;
        console.log('transactionCommitted: ', transactionCommitted);

        // Transaction completed
        session.endSession();

        // -------------------------
        // Saga Starts
        // -------------------------

        payment = await processPayment(booking);

        const seat = await reserveSeat(
            booking._id,
            "A10" // Change to FAIL for testing
        );

        const notification = await sendNotification(booking);

        booking.status = "CONFIRMED";
        await booking.save();
        return {
            booking,
            payment,
            seat,
            notification
        };

    }
    catch (err) {

        console.log(err.message);
        // Abort ONLY if local transaction
        // hasn't committed yet
        if (!transactionCommitted) {
            await session.abortTransaction();
            session.endSession();
        }

        // Saga Compensation
        if (payment) {
            await refundPayment(
                booking._id
            );
        }

        if (booking) {
            booking.status = "CANCELLED";
            await booking.save();
        }
        throw err;
    }
};

module.exports = {
    executeBookingSaga
};