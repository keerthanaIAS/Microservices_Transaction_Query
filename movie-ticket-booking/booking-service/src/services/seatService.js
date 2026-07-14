const axios = require("axios");

const reserveSeat = async (bookingId, seatNumber) => {
    const response = await axios.post(
        "http://localhost:3003/api/seats/reserve",
        {
            bookingId,
            seatNumber
        }
    );
    return response.data.seat;
};

module.exports = {
    reserveSeat
};