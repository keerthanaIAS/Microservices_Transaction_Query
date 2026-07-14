const { executeBookingSaga } = require("../services/bookingSaga");

const createBooking = async (req, res) => {
    try {
        const result = await executeBookingSaga(req.body);
        return res.status(201).json({
            success: true,
            ...result,
        });
    } catch (err) {
        return res.status(500).json({
            success: false,
            message: err.message,
        });
    }

};

module.exports = {
    createBooking,
};