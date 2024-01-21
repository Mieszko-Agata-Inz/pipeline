var express = require("express");
var path = require("path");
var logger = require("morgan");

var app = express();

app.use(logger("dev"));
app.use(express.json());
app.use(express.static(path.join(__dirname, "public-flutter")));
// add headers before the routes are defined
app.use(function (req, res, next) {
    // website to allow to connect
    res.setHeader('Access-Control-Allow-Origin', '*');
    // request methods to allow
    res.setHeader('Access-Control-Allow-Methods', 'GET');
    next();
});

setTimeout(function () {
    const port = 8080;
    app.listen(port, () => {
        console.log(`Server is running on port ${port}`);
    });
}, 70000); // 70 seconds of sleep just to wait for data in pipeline for first predictions

