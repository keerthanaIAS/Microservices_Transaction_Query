const mongoose = require("mongoose");

const Booking = require("../models/Booking");
const BookingLog = require("../models/BookingLog");

const Payment = require("../models/Payment");
const PaymentLog = require("../models/PaymentLog");

const Seat = require("../models/Seat");
const SeatLog = require("../models/SeatLog");

const bookTicket = async (req, res) => {

    const session = await mongoose.startSession();

    try {

        session.startTransaction();

        // -------------------
        // Booking
        // -------------------

        const bookings = await Booking.create(
            [{
                movieName: req.body.movieName,
                customerName: req.body.customerName,
                seats: req.body.seats,
                amount: req.body.amount,
                status: "CONFIRMED"
            }],
            { session }
        );

        const booking = bookings[0];

        await BookingLog.create(
            [{
                bookingId: booking._id,
                action: "BOOKING_CREATED"
            }],
            { session }
        );

        // -------------------
        // Payment
        // -------------------

        const payments = await Payment.create(
            [{
                bookingId: booking._id,
                amount: booking.amount,
                status: "SUCCESS"
            }],
            { session }
        );

        const payment = payments[0];

        await PaymentLog.create(
            [{
                paymentId: payment._id,
                bookingId: booking._id,
                action: "PAYMENT_SUCCESS"
            }],
            { session }
        );

        // -------------------
        // Seat
        // -------------------

        if (req.body.seatNumber === "FAIL") {
            throw new Error("Seat Reservation Failed");
        }

        const seats = await Seat.create(
            [{
                bookingId: booking._id,
                seatNumber: req.body.seatNumber,
                status: "RESERVED"
            }],
            { session }
        );

        const seat = seats[0];

        await SeatLog.create(
            [{
                seatId: seat._id,
                bookingId: booking._id,
                action: "SEAT_RESERVED"
            }],
            { session }
        );

        await session.commitTransaction();

        session.endSession();

        return res.json({
            success: true,
            booking,
            payment,
            seat
        });

    } catch (err) {

        await session.abortTransaction();

        session.endSession();

        return res.status(500).json({
            success: false,
            message: err.message
        });

    }

};

module.exports = {
    bookTicket
};