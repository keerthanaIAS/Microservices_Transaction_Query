const express = require("express");
const seatRoutes = require("./routes/seatRoutes");

const app = express();

app.use(express.json());

app.use("/api/seats", seatRoutes);

module.exports = app;