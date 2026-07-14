const Notification = require("../models/Notification");

const sendNotification = async(req,res)=>{
    const notification = await Notification.create(req.body);
    res.json({
        success:true,
        notification
    });
};

module.exports={
    sendNotification
};