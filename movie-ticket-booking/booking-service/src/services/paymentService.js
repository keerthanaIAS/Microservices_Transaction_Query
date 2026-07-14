const axios = require("axios");

const processPayment = async (booking) => {
    const response = await axios.post(
        "http://localhost:3002/api/payments/pay",
        {
            bookingId: booking._id,
            amount: booking.amount
        }
    );
    return response.data.payment;
};

const refundPayment = async (bookingId) => {
    const response = await axios.post(
        "http://localhost:3002/api/payments/refund",
        {
            bookingId
        }
    );
    return response.data.payment;
};

module.exports = {
    processPayment,
    refundPayment
};