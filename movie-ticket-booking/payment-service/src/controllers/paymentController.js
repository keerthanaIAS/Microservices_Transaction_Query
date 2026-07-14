const mongoose = require("mongoose");
const Payment = require("../models/Payment");
const PaymentLog = require("../models/PaymentLog");

const processPayment = async (req, res) => {
  const session = await mongoose.startSession();
  try {
    session.startTransaction();
    const payments = await Payment.create(
      [
        {
          bookingId: req.body.bookingId,
          amount: req.body.amount,
          status: "SUCCESS"
        }
      ],
      {
        session
      }
    );
    const payment = payments[0];
    await PaymentLog.create(
      [
        {
          paymentId: payment._id,
          bookingId: payment.bookingId,
          action: "PAYMENT_SUCCESS"
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
      payment
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

const refundPayment = async (req, res) => {
  const session = await mongoose.startSession();
  try {
    session.startTransaction();
    const payment = await Payment.findOne({
      bookingId: req.body.bookingId
    }).session(session);

    if (!payment) {
      await session.abortTransaction();
      session.endSession();
      return res.status(404).json({
        success: false,
        message: "Payment Not Found"
      });
    }

    payment.status = "REFUNDED";

    await payment.save({ session });
    await PaymentLog.create(
      [
        {
          paymentId: payment._id,
          bookingId: payment.bookingId,
          action: "PAYMENT_REFUNDED"
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
      payment
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
  processPayment,
  refundPayment
};