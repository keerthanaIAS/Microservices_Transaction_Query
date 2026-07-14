const express = require("express");
const router = express.Router();
const { reserveSeat } = require("../controllers/seatController");

router.post("/reserve", reserveSeat);

module.exports = router;