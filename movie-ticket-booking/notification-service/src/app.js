const express = require("express");
const notificationRoutes = require("./routes/notificationRoutes");

const app = express();

app.use(express.json());

app.use("/api/notifications", notificationRoutes);

module.exports = app;