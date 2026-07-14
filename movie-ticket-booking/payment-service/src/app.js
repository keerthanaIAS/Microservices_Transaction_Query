const express = require("express");
const paymentRoutes = require("./routes/paymentRoutes");

const app = express();

app.use(express.json());

app.use("/api/payments", paymentRoutes);

module.exports = app;