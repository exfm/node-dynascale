"use strict";

var Scale = require('./scale'),
    when = require('when'),
    sequence = require('sequence'),
    getConfig = require('junto'),
    express = require('express'),
    aws = require('plata');

var app = express(),
    scale = Scale.create();

scale.on('connected', function(){
    app.listen(8080);
    console.log('Listening on 8080');
});

scale.on('scale', function(data){
    console.log('Scaled table ' + data.tableName + ' by ' + data.scaledValue);
});

scale.on('err', function(err){
    throw err;
});

getConfig('development').then(function(config){
    scale.connect(config);
});

var writeActions = [
        'PutItem',
        'DeleteItem',
        'BatchWriteItem',
        'UpdateItem'
    ],
    readActions = [
        'GetItem',
        'BatchGetItem',
        'Scan',
        'Query'
    ],
    topic = aws.sns.Topic('dynascale');

function actionToOperation(action){
    if(writeActions.indexOf(action) > -1){
        return 'write';
    }

    if(readActions.indexOf(action) > -1){
        return 'read';
    }
    return null;
}

// SNS will send us a notification when we hit a throughput limit.
// Mambo will do this via a plugin any time a retry event is emitted.
// @todo (lucas) Also send to cloudwatch for finer grain resolution on whats
// happening / use alarms?
app.get('/sns-notification', topic.middleware(), function(req, res, next){
    var msg = req.notification.message,
        timestamp = (msg.timestamp) ? new Date(msg.timestamp) : new Date();
    scale.addException(msg.tableName, actionToOperation(msg.action),
        timestamp);
    res.send(200);
});

// Route to get throughput stats in JSON format
app.get('/throughput', function(req, res){
    scale.checkThroughput().then(function(data){
        res.json(data);
    });
});
