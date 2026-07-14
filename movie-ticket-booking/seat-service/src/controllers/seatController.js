const mongoose = require("mongoose");
const Seat = require("../models/Seat");
const SeatLog = require("../models/SeatLog");

const reserveSeat = async (req, res) => {
    const session = await mongoose.startSession();
    try {
        session.startTransaction();
        const { bookingId, seatNumber } = req.body;
        // Used only for Saga testing
        if (seatNumber === "FAIL") {
            throw new Error("Seat reservation failed");
        }
        const seats = await Seat.create(
            [
                {
                    bookingId,
                    seatNumber,
                    status: "RESERVED"
                }
            ],
            {
                session
            }
        );
        const seat = seats[0];
        await SeatLog.create(
            [
                {
                    seatId: seat._id,
                    bookingId: seat.bookingId,
                    action: "SEAT_RESERVED"
                }
            ],
            {
                session
            }
        );
        await session.commitTransaction();
        session.endSession();
        return res.status(201).json({
            success: true,
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

const releaseSeat = async (req, res) => {
    const session = await mongoose.startSession();
    try {
        session.startTransaction();
        const seat = await Seat.findOne({
            bookingId: req.body.bookingId
        }).session(session);
        if (!seat) {
            await session.abortTransaction();
            session.endSession();
            return res.status(404).json({
                success: false,
                message: "Seat Not Found"
            });
        }

        seat.status = "RELEASED";

        await seat.save({ session });
        await SeatLog.create(
            [
                {
                    seatId: seat._id,
                    bookingId: seat.bookingId,
                    action: "SEAT_RELEASED"
                }
            ],
            {
                session
            }
        );
        await session.commitTransaction();
        session.endSession();
        return res.json({
            success: true,
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
    reserveSeat,
    releaseSeat
};