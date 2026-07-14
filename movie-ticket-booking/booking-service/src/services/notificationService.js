const axios=require("axios");

const sendNotification=async(booking)=>{
    const response=await axios.post(
        "http://localhost:3004/api/notifications/send",
        {
            bookingId:booking._id,
            message:"Your ticket has been booked successfully."
        }
    );
    return response.data.notification;
};

module.exports={
    sendNotification
};