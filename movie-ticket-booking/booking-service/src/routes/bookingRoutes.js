const express = require("express");

const router = express.Router();

const {
  createBooking,
} = require("../controllers/bookingController");
const { bookTicket } = require("../services/transactionService");

router.post("/book-ticket", createBooking);
router.post(
    "/transaction-booking",
    bookTicket
);

module.exports = router;