"use strict";

var gauss = require('gauss'),
    util = require('util');

var now = Date.now(),
    data = {
        'Feed': {
            'read': [],
            'write': []
        }
    };

var trendingDown = [1, 2, 3, 2, 0, 0, 0, 0, 0, 0];
var trendingUp =   [1, 2, 0, 0, 1, 2, 3, 4, 6, 10];
var trendingMeh =   [1, 2, 0, 0, 1, 2, 0, 0, 2, 0];
var windowLength = 5;

trendingMeh.forEach(function(s, i){
    data.Feed.write.push([new Date(now + i), s]);
});

data.Feed.write = new gauss.TimeSeries(data.Feed.write);
var ema = data.Feed.write.values().ema(windowLength);
ema.reverse();

var direction,
    scaleTo = 0;

if(ema[0] > ema[ema.length - 1]){
    direction = 'up';
    scaleTo = Math.ceil(Math.max.apply(ema, ema));
}

if(direction){
    console.log('We should scale ' + direction + ' ' +  scaleTo + ' units');
}
else {
    console.log('Do nothing.');
}
