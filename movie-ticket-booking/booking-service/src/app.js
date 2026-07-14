const express = require("express");
const bookingRoutes = require("./routes/bookingRoutes");

const app = express();

app.use(express.json());

app.use("/api/bookings", bookingRoutes);

app.get("/", (req, res) => {
    res.json({
        service: "Booking Service",
        status: "Running"
    });
});

module.exports = app;